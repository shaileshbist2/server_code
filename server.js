/***************************** IMPORTS *****************************************/
const express = require('express');
const bodyParser = require('body-parser');
const path = require('path');
const app = express();
const cliMsg = require('./utils/cli-msg/node-msg');
const serverConfig = require('./config/serverDynamicConfig.json');
const mongoose = require('mongoose');
const passport = require('passport');
const server = require('http').Server(app);
const io = require('socket.io')(server);
const methodOverride = require('method-override');
/*******************************************************************************/

/********************** Server Port Setting ************************************/
let envMsg, port = process.env.PORT || serverConfig.defaultServerPort;
/*******************************************************************************/

/*********************** Body Parser ********************************************/
app.use(bodyParser.urlencoded({ extended: true }));
app.use(bodyParser.json({ limit: '50mb' }));
app.use(methodOverride('_method'));
/*******************************************************************************/

/******************* Server Access Control ************************************/
app.use(function (req, res, next) {
    res.set("Access-Control-Allow-Origin", "*");
    res.set("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
    res.set("Access-Control-Allow-Credentials", "true");
    res.set("X-Frame-Options", "DENY");
    next();
});
/******************************************************************************/

/*********************** Soket Config **************************************/
require("./utils/socketConfig")(io);
/******************************************************************************/

/*************************** API Caller **************************************/
app.use('/api', require('./routes'));
/*****************************************************************************/

/************************* Mongo DB *******************************************/
mongoose
    .set('useFindAndModify', false)
    .connect(serverConfig.MONGO_URI, { useNewUrlParser: true, useUnifiedTopology: true, useCreateIndex: true })
    .then(() => console.log('mongoDB Connected'))
    .catch(err => console.log(err));
/******************************************************************************/

/*********************** Passport Config **************************************/
app.use(passport.initialize());
require('./utils/passport')(passport);
/******************************************************************************/


/********************* ENV PROD & DEV **************************************/
if (process.env.NODE_ENV === 'prod') {
    envMsg = serverConfig.handleMessage.prodMsg;
    app.use(express.static(serverConfig.buildFolderPath));
    app.get('/*', function (req, res) {
        res.sendFile(path.resolve(__dirname, serverConfig.buildFilePath));
    });
} else {
    envMsg = serverConfig.handleMessage.devMsg;
}
/****************************************************************************/

/************************** TABLE MSG ***************************************/
if (serverConfig.handleControl.cliServerMsg) {
    cliMsg.serverMessage({
        ServerPort: port,
        Message: envMsg,
        DateTime: new Date()
    });
}
/****************************************************************************/

//app.listen(port);
server.listen(port);