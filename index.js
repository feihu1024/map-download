const fs = require('fs');

const utils = require('./js/utils');
const tdtDownload = require('./js/tdt-download');

const args = process.argv.splice(2);

let configPath = args[0];
if (!configPath) {
    configPath = './config/' + fs.readdirSync('./config')[0];
}

if (!configPath) {
    console.log('程序已终止：无配置文件！');
    return;
}

// 读取配置文件
let config = utils.readJson(configPath);
if (!config) {
    console.log('程序已终止：无法读取配置文件！');
}

const task = config.taskList.find((item) => item.complete === false);
if (!task) {
    console.log('程序已终止：目标任务不存在！');
}

const [minX, minY, maxX, maxY] = task.metadata.bounds;
const bounds = { minX, minY, maxX, maxY };
for (let index = task.metadata.minzoom; index <= task.metadata.maxzoom; index++) {
    const tileRange = utils.bounds2Tile(bounds, index);
    const row = tileRange.maxX - tileRange.minX + 1;
    const col = tileRange.maxY - tileRange.minY + 1;
    console.log(index, tileRange, `row:${row}: col:${col} tiles:${row * col}`);
}

if (fs.existsSync('./logs')) {
    fs.rmSync('./logs', { recursive: true });
}

tdtDownload.startTask(task, config, configPath);
