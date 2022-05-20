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
      if(parseInt(row[2])> 0 ){
        jobs.push(new Job(parseInt(row[0]),parseInt(row[1]), parseInt(row[2])));
       
      }
   
  }).on("end", function () {
        
    let tasks = schedule(jobs);
    tasks = getScheduledJOBs(tasks)

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
        if (jobs[mid].finish <= start){
            if(jobs[mid + 1].finish <= start)
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

jobs.sort((a,b)=>a.finish - b.finish)
let tasks =  [];
let firstJob = new Job(0,0,0);
tasks.push(new TaskJob(0,0,firstJob));

let n = jobs.length;

for (var i = 0; i < n; i++)
{
let len = tasks.length;
let currentPtr = len - 1;
let tmp = tasks[currentPtr];
if (jobs[i].start > tmp.finish){
    tasks.push(new TaskJob(currentPtr,jobs[i].profit + tmp.currentProfit,jobs[i]));
}
else if (jobs[i].start == tmp.finish){
    if(jobs[i].profit > tmp.profit){
        tasks.push(new TaskJob(tasks[currentPtr].ptr,jobs[i].profit + tmp.currentProfit-tmp.profit,jobs[i]));
    }
}
else{
        let index = binarySearch(tasks,jobs[i].start );
        if(index != -1){
            let profit = 0;
            if(jobs[i].start > tasks[index].finish && (profit=(jobs[i].profit + tasks[index].currentProfit)) > tmp.currentProfit){
                tasks.push(new TaskJob(index,profit,jobs[i]));
            }
        }
}

}
    return tasks;
}
 
function getScheduledJOBs(jobs){
    let item = jobs[jobs.length-1];
    let schedule = [];
    while (item.ptr){
        schedule.push([item.start,item.finish,item.profit]);
        item = jobs[item.ptr];
    }
    schedule.push([item.start,item.finish,item.profit]);
    return schedule.reverse();
}

 