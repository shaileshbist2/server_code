const Table = require('tty-table');

const serverMessage = (arg) => {
    const options = {
        borderStyle: 2,
        borderColor: "blue",
        truncate: "...",
    };
    const rows = [arg];
    let header = [
        {
            alias: "Server Port",
            value: "ServerPort",
            headerColor: "cyan",
            color: "white",
            width: 20
        },
        {
            value: "Message",
            color: process.env.NODE_ENV === 'prod' ? "green" : 'red',
            width: 45
        },
        {
            alias: "Date Time",
            value: "DateTime",
            width: 30
        }
    ];
    console.log(Table(header, rows, options).render());
};

module.exports = {
    serverMessage
};
