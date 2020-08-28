const express = require('express');
const router = express.Router();
const User = require('../../models/User');
const bcryptjs = require('bcryptjs');
const {createResponse} = require('../../utils/res');
const {inputValidation} = require('../../utils/validate');

router.post('/', (req, res) => {
    const validate = inputValidation(req.body, ['username', 'password', 'userType']);
    if (validate.error) {
        return createResponse(req, res, true, '', validate);
    } else {
        User.findOne({username: req.body.username}).then(user => {
            if (user) {
                return createResponse(req, res, true, 'username already exists', {});
            } else {
                const newUser = new User({
                    username: req.body.username,
                    password: req.body.password,
                    userType: req.body.userType
                });
                /****************************** bcryptjs with salt **********************************************/
                bcryptjs.genSalt(10, (err, salt) => {
                    bcryptjs.hash(newUser.password, salt, (err, hash) => {
                        if (err) {
                            return createResponse(req, res, true, err.message, {})
                        } else {
                            newUser.password = hash;
                            newUser.save().then(user => {
                                user.password = null;
                                return createResponse(req, res, false, 'login successfully', user)
                            }).catch(err => {
                                return createResponse(req, res, true, err.message, {})
                            });
                        }
                    });
                });
            }
        })
    }
});

module.exports.router = router;
