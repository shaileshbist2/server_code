const mongoose = require('mongoose');
const path = require('path');
const multer = require('multer');
const crypto = require('crypto');
const GridFsStorage = require('multer-gridfs-storage');
const Grid = require('gridfs-stream');
const serverConfig = require('../config/serverDynamicConfig.json');
const conn = mongoose.connection;
//let gfs;
// conn.once('open', (req, res) => {
//     // Init stream
//     gfs = Grid(conn.db, mongoose.mongo);
//     gfs.collection('admin');
//     console.log('gfs ========== >>>> ', gfs);
//    //  resolve(gfs);
// });

const gfs = () => {
    return Grid(conn.db, mongoose.mongo);
}

// Create storage engine
const storage = new GridFsStorage({
    url: serverConfig.MONGO_URI,
    file: (req, file) => {
        let username;
        return new Promise((resolve, reject) => {
            if (req.user.username) {
                crypto.randomBytes(16, (err, buf) => {
                    if (err) {
                        return reject(err);
                    }
                    // const filename = buf.toString('hex') + path.extname(file.originalname);
                    const fileInfo = {
                        filename: file.originalname,
                        bucketName: req.user.username
                    };
                    resolve(fileInfo);
                });
            } else {
                reject();
            }
        });
    }
});

const upload = multer({ storage });

module.exports = { upload, gfs };
