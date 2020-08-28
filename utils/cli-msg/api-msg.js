const table = require('tty-table');

const options = {
    borderStyle: 1,
    borderColor: "green"
};

const apiMessage = (data, api) => {
    data = {
        ...data,
        response: api + '\n\n' + JSON.stringify(data.response),
        datetime: new Date()
    };
    const rows = [data];
    let header = [
        {
            alias: "Date Time",
            value: 'datetime',
            width: 20
        },
        {
            alias: "Error",
            value: "error",
            headerColor: "cyan",
            color: data.error ? 'red' : 'green',
            width: 10
        },
        {
            alias: "Message",
            value: "message",
            headerColor: "cyan",
            color: data.error ? 'red' : 'white',
            width: 20
        },
        {
            alias: "Response",
            value: "response",
            headerColor: "cyan",
            color: data.error ? 'red' : 'white',
            width: 80
        }
    ];
    console.log(`${table(header, rows, options).render()}`);
};

module.exports = {
    apiMessage
};
