const express = require('express');
const router = express.Router();
const passport = require('passport');
const reqResUtility = require('../../utils/res');
const { upload, gfs } = require('../../controllers/grid-fs');

router.post('/isFileExist', passport.authenticate('jwt', { session: false }), async (req, res) => {
    let ob = gfs();
    ob.collection(req.user.username);
    const data = await ob.files.findOne({ filename: req.body.filename });
    if (data === null) {
        reqResUtility.createResponse(req, res, false, 'success', false);
    } else {
        reqResUtility.createResponse(req, res, false, 'success', true);
    }
});

router.post('/delete_file', passport.authenticate('jwt', { session: false }), async (req, res) => {
    let ob = gfs();
    ob.collection(req.user.username);
    ob.remove({ _id: req.body.id, root: req.user.username }, (err, gridStore) => {
        if (err) {
            return res.status(404).json({ err: err });
        }
        reqResUtility.createResponse(req, res, false, 'success', {});
    });
});

router.post('/upload_file', passport.authenticate('jwt', { session: false }), upload.single('file'), async (req, res) => {
    console.log('req.file ==>> ', req.file);
    reqResUtility.createResponse(req, res, false, 'success', req.file);
});

router.get('/:user/:filename', (req, res) => {
    let ob = gfs();
    ob.collection(req.params.user);
    ob.files.findOne({ filename: req.params.filename }, (err, file) => {
        if (!file || file.length === 0) {
            return res.status(404).json({
                err: 'No file exists'
            });
        }
        const readstream = ob.createReadStream(file.filename);
        readstream.pipe(res);
    });
});

module.exports.router = router;