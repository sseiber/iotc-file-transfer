// Azure function to handle file uploads through IoT Central's data export feature
// built-in modules
const fs = require('fs');
const path = require('path');

// open source modules that need to be npm installed
const glob = require('glob');
const zlib = require('zlib');

module.exports = async function (context, req) {
    const baseDir = path.join('c:', 'home', 'site', 'wwwroot', 'upload', 'files');
    const tempDir = path.join(baseDir, 'temp-uploads');
    const uploadDir = path.join(baseDir, 'file-uploads');
    const deadLetterDir = path.join(baseDir, 'dead-letter');

    // variables
    const deadLetterExpireTimeInHours = 12;
    let statusCode = 200;
    let errorMessage = '';

    try {
        // make sure the needed directories are available
        if (!fs.existsSync(tempDir)) {
            fs.mkdirSync(tempDir, { recursive: true });
        }

        if (!fs.existsSync(deadLetterDir)) {
            fs.mkdirSync(deadLetterDir, { recursive: true });
        }

        if (!fs.existsSync(uploadDir)) {
            fs.mkdirSync(uploadDir, { recursive: true });
        }

        const deviceId = req.body.deviceId;

        // pull the message part meta data from the message properties
        const id = req.body.messageProperties.id || '';
        if (!id) {
            throw new Error('Missing message property: id');
        }

        const filepathProp = req.body.messageProperties.filepath || '';
        if (filepathProp === '.') {
            throw new Error('Missing message property: filepath');
        }

        const filepath = path.dirname(filepathProp);
        const filename = path.basename(path.normalize(filepathProp));
        const filenameBaseNoExt = filename.split('.').slice(0, -1).join('.');
        const filenameExt = path.extname(filename);

        if (!req.body.messageProperties.part && req.body.messageProperties.part !== 0) {
            throw new Error('Missing message property: part');
        }
        const part = Number(req.body.messageProperties.part);

        if (!req.body.messageProperties.maxPart) {
            throw new Error('Missing message property: maxPart');
        }
        const maxPart = Number(req.body.messageProperties.maxPart);

        if (!req.body.messageProperties.compression) {
            throw new Error('Missing message property: compression');
        }
        const compression = req.body.messageProperties.compression.toLowerCase();
        if (compression !== 'none' && compression !== 'deflate') {
            context.log(`compression message property is invalid, received: ${compression}`);
        }

        // log new file part
        context.log.info(`device_id ${deviceId} file_id: ${id} part: ${part} of: ${maxPart} filepath: ${filepath} filename: ${filename}`);

        // write out the file part
        fs.writeFileSync(path.join(tempDir, `${deviceId}.${id}.${maxPart}.${part}`), req.body.telemetry.contentChunk);

        // check to see if all the file parts are available
        const filePartCount = glob.sync(path.join(tempDir, `${deviceId}.${id}.*`)).length;
        if (filePartCount === maxPart) {
            // all expected file parts are available - time to rehydrate the file
            const encodedData = [];
            for (let i = 1; i <= maxPart; i++) {
                const chunk = fs.readFileSync(path.join(tempDir, `${deviceId}.${id}.${maxPart}.${i}`));
                encodedData.push(chunk);
            }

            const buff = Buffer.from(encodedData.join(''), 'base64');
            const dataBuff = compression === 'deflate' ? zlib.inflateSync(buff) : buff;

            // write out the rehydrated file
            const fullUploadDir = path.join(uploadDir, filepath);
            if (!fs.existsSync(fullUploadDir)) {
                fs.mkdirSync(fullUploadDir, { recursive: true });
            }

            let currentFilename = filename;
            if (fs.existsSync(path.join(fullUploadDir, filename))) {
                // create a revision number between filename and extension
                const filesExistingCount = glob.sync(path.join(fullUploadDir, `${filenameBaseNoExt}.**${filenameExt}`)).length;
                currentFilename = `${filenameBaseNoExt}.${filesExistingCount + 1}${filenameExt}`;
            }

            context.log.info(`writing out the file: ${currentFilename}`);

            fs.writeFileSync(path.join(fullUploadDir, currentFilename), dataBuff);

            // clean up the message parts
            for (let i = 1; i <= maxPart; i++) {
                const tempFilename = path.join(tempDir, `${deviceId}.${id}.${maxPart}.${i}`);

                try {
                    fs.unlinkSync(tempFilename);
                }
                catch (ex1) {
                    // pause and try this again incase there was a delay in releasing the file lock
                    try {
                        context.log.warn(`Failure whilst cleaning up a temporary file retrying: ' + ${tempFilename} - ${ex1.message}`);

                        await new Promise((resolve) => {
                            setTimeout(() => {
                                fs.unlinkSync(tempFilename);

                                return resolve('');
                            }, 100);
                        });
                    }
                    catch (ex2) {
                        // failed a second time so log the error, the file will be caught and dead lettered at a later time
                        context.log.error(`Error whilst cleaning up a temporary file: ${tempFilename} - ${ex2.message}`);
                    }
                }
            }
        }

        // check for expired files in temp directory and dead letter them
        const files = fs.readdirSync(tempDir);
        const dt = new Date();
        dt.setHours(dt.getHours() - deadLetterExpireTimeInHours);

        for (const file of files) {
            // a race condition can happen here where a file has been deleted after the list of files has been collected, handled in the exception catch
            try {
                const { birthtime } = fs.statSync(path.join(tempDir, file));
                if (dt > birthtime) {
                    fs.renameSync(path.join(tempDir, file), path.join(deadLetterDir, file));
                }
            }
            catch (exExpired) {
                // none essential exception this will be called again so just log it
                context.log.warn(`Exception occured during dead-letter cleanup. Details: ${exExpired.message}`);
            }
        }
    }
    catch (ex) {
        // log any exceptions as errors
        errorMessage = ex.message;
        statusCode = 500;

        context.log.error(`Exception thrown: ${errorMessage}`);
    }
    finally {
        // return success or failure
        context.res = {
            status: statusCode,
            body: errorMessage
        };

        context.done();
    }
};
