const express = require('express');
const router = express.Router();
const User = require('../../models/User');
const bcryptjs = require('bcryptjs');
const jwt = require('jsonwebtoken');
const {jwtSecret, jwtSignInExpiresIn} = require('../../config/serverDynamicConfig.json');
const {createResponse} = require('../../utils/res');
const {inputValidation} = require('../../utils/validate');
const jwt_decode = require("jwt-decode");

router.post('/', (req, res) => {
    const decoded = jwt_decode(req.body.token);
    [req.body.username, req.body.password] = [decoded.username, decoded.password];
    const validate = inputValidation(req.body, ["username", "password"]);
    if (validate.error) {
        return createResponse(req, res, true, '', validate);
    } else {
        User.findOne({username: req.body.username}).then(user => {
            if (!user) {
                return createResponse(req, res, true, '', {username: 'username not found!'});
            } else {
                bcryptjs.compare(req.body.password, user.password).then(isMatch => {
                    if (isMatch) {
                        const payload = {id: user.id, username: user.username, userType: user.userType};
                        jwt.sign(payload, jwtSecret, {expiresIn: jwtSignInExpiresIn}, (err, token) => {
                            return createResponse(req, res, false, 'successfully logged-in', {token: 'Bearer ' + token});
                        })
                    } else {
                        return createResponse(req, res, true, '', {password: 'password is not valid!'});
                    }
                })
            }
        })
    }
});

module.exports.router = router;
