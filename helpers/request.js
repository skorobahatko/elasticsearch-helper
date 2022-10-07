const util = require('util');
const axios = require('axios').default;
const _exec = require('child_process');

const executeCommand = util.promisify(_exec.exec);

const httpRequest = async (method, url, data) => {
  let response;
  try {
    response = await axios.request({ method, headers: { host: url.replace(/https?:\/\//, ''), 'Content-Type': 'application/json' }, data });
  } catch (err) {
    console.log(err.response?.data);

    throw new Error(err.response?.data?.message || err.response?.data?.error?.message || err.message);
  }
  return response;
};

const curlRequest = async (method, url, data) => {
  let response;
  try {
    response = await executeCommand(`curl -X ${method} "${url}" -H 'Content-Type: application/json' -d '${data}\n'`);
  } catch (err) {
    console.log(err.message);
    throw new Error(err.response?.stdout?.message || err.response?.error?.message || err.message);
  }
  return response?.stdout;
};

module.exports = {
  httpRequest,
  curlRequest
};
