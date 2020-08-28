const inputValidation = (data, fields) => {
    let output = {};
    for (let value of fields) {
        if (!Object.keys(data).includes(value)) {
            output[value] = value + ' is required!';
        } else if (data[value] === '') {
            output[value] = value + ' should not be empty!';
        }
    }
    output['error'] = Object.keys(output).length !== 0;
    return output;
};

module.exports = {
    inputValidation
};
