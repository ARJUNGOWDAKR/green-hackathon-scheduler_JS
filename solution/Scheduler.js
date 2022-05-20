const fs = require("fs");
const { parse } = require("csv-parse");
const {stringify } = require('csv-stringify') ;

const start = Date.now();
let jobs = [];
class Job{
    constructor(start, finish, profit){
        this.start = start
        this.finish = finish
        this.profit = profit
    }
}

class TaskJob extends Job {
    constructor(ptr,currentProfit, job) {
        super(job.start,job.finish, job.profit);
        this.ptr = ptr;
        this.currentProfit = currentProfit;
    }
};
const args = process.argv.slice(2);
let input = args[0]; //"C:\\Apps\\green-hackathon-scheduler_JS\\test_data\\test_data_final.csv"
let outputFile = args[1]; // './my.csv'
//console.log(outputFile);
//console.log(input);
fs.createReadStream(input)
  .pipe(parse({ delimiter: ",", from_line: 2 }))
  .on("data", function (row) {
     // console.log(parseInt(row[2]));
      if(parseInt(row[2])> 0 ){
        jobs.push([parseInt(row[0]),parseInt(row[1]), parseInt(row[2])]);
       
      }
   
  }).on("end", function () {
   // console.log(jobs.length);
    let tasks = schedule(jobs);
   // console.log(tasks.length);
    tasks = getScheduledJOBs(tasks);

    stringify(tasks, { header: false}, (err, output) => {
        if (err) throw err;
        fs.writeFile(outputFile, output, (err) => {
          if (err) throw err;
            const stop = Date.now();
            console.log(`Time Taken to execute = ${(stop - start)/1000} seconds`);
    
        });
      });
    
  })

 
function binarySearch(jobs, start){
    let lo = 0
    let hi = jobs.length;

    while(lo <= hi){
        let mid = Math.floor((lo + hi) /2);
        //console.log(jobs[mid]);
        if (jobs[mid][2][1] <= start){
            if(jobs[mid + 1][2][1] <= start)
                lo = mid + 1
            else
                return mid
        }
        else
            hi = mid - 1
    }
 
    return -1
 
}
 

function schedule(jobs){
    
jobs.sort((a,b)=>a[1] - b[1]);
let tasks =  [];

//let firstJob = new Job(0,0,0);
tasks.push([0,0,[0,0,0]]);

let n = jobs.length;

for (var i = 0; i < n; i++)
{
let len = tasks.length;
let currentPtr = len - 1;
let tmp = tasks[currentPtr];
//console.log(tmp);
if (jobs[i][0] > tmp[2][1]){
    tasks.push([currentPtr,jobs[i][2] + tmp[1],jobs[i]]);
}
else if (jobs[i][0] == tmp[2][1]){
    if(jobs[i][2] > tmp[2][2]){
        tasks.push([tasks[currentPtr][0],jobs[i][2] + tmp[1] - tmp[2][2],jobs[i]]);
    }
}
else{
    
        let index = binarySearch(tasks,jobs[i][0] );
        if(index != -1){
            let profit = 0;
            if(jobs[i][0] > tasks[index][2][1] && (profit=(jobs[i][2] + tasks[index][1])) > tmp[1]){
                tasks.push([index,profit,jobs[i]]);
            }
        }
        
}

}
    return tasks;
}
 
function getScheduledJOBs(jobs){
    let item = jobs[jobs.length-1];
    let schedule = [];
    while (item[0]){
        schedule.push([item[2][0],item[2][1],item[2][2]]);
        item = jobs[item[0]];
    }
    schedule.push([item[2][0],item[2][1],item[2][2]]);
    return schedule.reverse();
}

 