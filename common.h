//
// Created by Kuo Zhang on 6/14/20.
//

#ifndef CPP_COMMON_H
#define CPP_COMMON_H

//#define DEBUG_ALG
//#define DEBUG_DISPATCH
//#define ENQUEUE_INFO
//#define DISPATCH_FAILURE

//#define NDEBUG

#include <cassert>
#include <unordered_map>
#include <map>
#include<vector>
#include <deque>
#include <set>
#include <functional>
#include <tuple>
#include <memory>
#include <utility>
#include <list>
#include <numeric>
#include <string>
#include <unordered_set>
#include "pprint.h"
#include "spdlog/spdlog.h"
#include "spdlog/cfg/env.h"
#include "spdlog/sinks/basic_file_sink.h"
#include "spdlog/sinks/stdout_color_sinks.h"

using std::string;
using std::vector;
using std::unordered_map;
using std::unordered_set;
using std::map;
using std::tuple;
using std::pair;
using std::make_tuple;
using std::make_pair;
using std::unique_ptr;
using std::multiset;

extern pprint::PrettyPrinter print;

const int MAX_NONDF_DISPATCH_FAIL = 1;
const int CAP_VAL_NEAREST_DDL = 12*12; // in slot
const double IDLE_SUSPEND_DIFFERENCE = 16.0; // 4w and 20w

struct Env{
    // state info that will be passed to RL agent
    double next_temp;
    double cluster_utilization; //cluster-wide
    long num_df_dispatched; // during a slot
    long num_nondf_dispatched;

    long nondf_wait_t_max; // during a slot
    long nondf_wait_t_min; // during a slot
    double nondf_wait_t_total; // during a slot
    long nondf_wait_t_cnt ; // how many cnt of tasks correspond to the previous "total"


    double instant_on_server_utilization;
    long total_cores_current_on;
    long OldestNonDfTaskArrivalTimeDistance;
};
struct Task {
    const long id;
    const string type;
    //--------------state--------------
    const long arr_t;
    long dispatch_t; // -1 initialized
    const long start_ddl; // expected to be longeger times of slot_duration
    long finish_t;
    long assigned_svr; //-1 initialized
    const long duration;
    const long resource_req;

    long dispatch_attempt_cnt;
    long last_dispatch_failure_time;

    explicit Task(long id, std::string ty, const long dur, long req, long arr_t, long start_ddl) :
            id{id},
            type(std::move(ty)),
            arr_t(arr_t),
            dispatch_t(-1),
            start_ddl(start_ddl),
            finish_t(-1),
            assigned_svr(-1),
            duration(dur),
            resource_req(req),
            dispatch_attempt_cnt(0),
            last_dispatch_failure_time(-1){};
    friend std::ostream& operator<<(std::ostream& os, const Task& tsk){
        pprint::PrettyPrinter printer(os);
        printer.print("ID:",tsk.id,"startDDL:",tsk.start_ddl,"req",tsk.resource_req,
                "dura:",tsk.duration,"type:",tsk.type,
                "arrT:",tsk.arr_t,"dispatchT:",tsk.dispatch_t,"finishT:",
                tsk.finish_t,"assignedSvr:",tsk.assigned_svr,"attempt#:",tsk.dispatch_attempt_cnt,"lastFailT:",
                tsk.last_dispatch_failure_time);
        return os;
    }

    void modifyStateOnAssign(const long dispatch_time, const long svr_assigned){
        assert((dispatch_time >=arr_t));
        dispatch_t = dispatch_time;
        finish_t = dispatch_t + duration;
        assigned_svr = svr_assigned;
    }
    void markDispatchFailure(long dispatch_attempt_time){
        ++dispatch_attempt_cnt;
        last_dispatch_failure_time =dispatch_attempt_time;
    }
    long getNumDispatchAttemptFailure() const {return dispatch_attempt_cnt;}
    long getWaitingTime() const {
        // must be called after dispatch_t is set
        return dispatch_t - arr_t;
    }

};

class Server {
    std::unordered_set<long> task_set;
    const long name;
    const string type;
    // {on, off, wkup,unschedulable }
    long on_off; // when this is changed, available resources must be changed
    const long total_cores;
    long avail_cores;

    long cumu_core_used_time; // cumulative used_cores * span
    long last_update_time; // time that this server accepts or removes a task (initialized as slot start time)
    long cumu_time; // the cumu_time which corresponds to the cumulative utilization

public:
//    Server(long total_cores):total_cores(total_cores){};

    Server(long name, string type, long total_cores, long on_off) :
            name(name),
            type(std::move(type)),
            on_off(on_off),
            total_cores(total_cores),
            avail_cores(total_cores),
            cumu_core_used_time(0) {};

    // NOTE, acceptTask and removeTask are the ONLY function that modifies: avail_cores
    void acceptTask(Task*t,long dispatch_time) {
        t->modifyStateOnAssign(dispatch_time,this->name);
        cumu_time+= (dispatch_time - last_update_time);
        cumu_core_used_time+= ((total_cores - avail_cores) * (dispatch_time - last_update_time));
        last_update_time = dispatch_time;
        task_set.insert(t->id);
        avail_cores -= t->resource_req;
    }

    void removeTask(const Task *t) {
        cumu_time+=(t->finish_t - last_update_time);
        cumu_core_used_time+= (total_cores - avail_cores) * (t->finish_t - last_update_time);
        last_update_time = t->finish_t;
        avail_cores += t->resource_req;
        task_set.erase(t->id);
    }
    void setOn(){on_off=1;}
    void setOff(){on_off=0;}
    void setUnschedulable(){on_off=2;}
    // assuming wake up process needs "cores_cnt" cores, to simulate the wake-up cost
    void markWakingUp(long cores_cnt) { on_off=3;avail_cores-=cores_cnt;}
    void setOnFromWakeUp() {on_off=1;avail_cores=total_cores;}
    bool isOn() const {return on_off==1;}
    bool isOff() const {return on_off==0;}
    bool isUnschedulable() const {return on_off==2;}
    long getTotalCores() const {return total_cores;}
    long getAvailCores() const {return avail_cores;}
    long getState() const {return on_off;}

    void resetCumuUtil(long curr_time) {
        // reset the start point from which an average utilization will be computed
        cumu_core_used_time=0;
        last_update_time=curr_time;
        cumu_time=0;}

    double getCumuUtilizationUntil(long until_time){
        // get average utilization between the latest reset time and the this method's invocation time (until_time)
        cumu_time+= (until_time - last_update_time);
        cumu_core_used_time+= (total_cores-avail_cores)*(until_time - last_update_time);
        last_update_time = until_time;
        return ( static_cast<double>(cumu_core_used_time)/ static_cast<double>(total_cores) )/ static_cast<double>(cumu_time);
    }

    double getInstantUtilization() const {
        return static_cast<double>((total_cores - avail_cores)) / static_cast<double>(total_cores);
    }

    long getLastFinishedTaskTime (const unordered_map<long, std::unique_ptr<Task>>& id_tsk_mp,long curr_time) const {
        // if there is no task in the server, return `curr_time`
        long last_finish_time = curr_time;
        for (const auto& task_id:task_set){
            if (id_tsk_mp.at(task_id).get()->finish_t > last_finish_time){
                last_finish_time = id_tsk_mp.at(task_id).get()->finish_t;
            }
        }
        return last_finish_time;
    }
    friend std::ostream& operator<<(std::ostream& os, const Server& svr){
        pprint::PrettyPrinter printer(os);
        printer.compact(true);
        printer.line_terminator("");
        printer.print("onOff:",svr.on_off,"AvailCores:",svr.avail_cores,"taskset:",svr.task_set," ");
        printer.line_terminator("\n");
        printer.print("CumuCoreMultiTime:",svr.cumu_core_used_time,"CumuTime:",svr.cumu_time,
                "LastUpdateTime:",svr.last_update_time);
        return os;
    }




};
//class ddlComp{
//public:
//    bool operator() (Task*t1,Task*t2){return t1->start_ddl<t2->start_ddl;};
//};

// defer
static auto comp_wait_q = [](Task *t1, Task *t2) {
    // for req, it is ascending order
    if (t1->start_ddl < t2->start_ddl) {
        return true;
    } else if (t1->start_ddl > t2->start_ddl) {
        return false;
    } else if (t1->resource_req > t2->resource_req){
        return true;
    }else if (t1->resource_req < t2->resource_req){
        return false;
    }else{
        return t1->arr_t < t2->arr_t; // task with small arr_t (older task) will be at first
    }
};
static auto comp_running_q = [](Task *t1, Task *t2) {
    return t1->finish_t < t2->finish_t;
};

//struct RunningTaskQueue{
//    std::multiset<Task*, decltype(comp_running_q)> run_q;
//
//    RunningTaskQueue():run_q(comp_running_q){};
//};
struct NoneDeferWaitTaskQueue {
    std::list<Task *> q;
    std::vector<long> waiting_time;

   friend std::ostream& operator<<(std::ostream& os, const NoneDeferWaitTaskQueue& q){
       pprint::PrettyPrinter printer(os);
       printer.print("--------NoneDeferTaskQueueStart--[old->new]---------");
       for(auto t_ptr:q.q){os<<*t_ptr;}
       return os;
   }

   tuple<double,long,long,long> getAveWaitingTime() const {
       // For all DISPATCHED Tasks not including current waiting task
       if (waiting_time.empty()){return {0,0,0,0};}
       double ave= std::accumulate(waiting_time.begin(),waiting_time.end(),0.0)/waiting_time.size();
       long max = *std::max_element(waiting_time.begin(),waiting_time.end());
       long min = *std::min_element(waiting_time.begin(),waiting_time.end());
       return {ave,max,min,waiting_time.size()};
   }
   tuple<long,long,long,long> getToalWaitingTimeIncludeUndispatched(long curr_time) const{
       // For all DISPATCHED Tasks AND  current waiting task
       long total_wait_t = 0;
       long max_wait_time =0; // maximum waiting time for all tasks including dispatched and tasks in queue
       for (auto wt:waiting_time){
           if (wt>max_wait_time){max_wait_time=wt;}
           total_wait_t+=wt;
       }
       if (!q.empty()){assert((q.back()->arr_t < curr_time));} // all task in q must have arrival time  <  current time
       for (auto tsk:q){
           if (curr_time-tsk->arr_t > max_wait_time){ max_wait_time=curr_time-tsk->arr_t;}
           total_wait_t+=curr_time-tsk->arr_t;
       }
       return {total_wait_t,waiting_time.size()+q.size(),max_wait_time,waiting_time.size()};
   }

    void enqueueNonDf(Task *t) {
        q.push_back(t); //new task at end, while dequeue from front (old)  [old, new]
    }
    long getOldestTaskArrivalTimeDistance(long curr_time){
        //return curr_time - arrival_time_of_oldest_task
        if (q.empty()) {return 0;}
        assert( (curr_time >(*q.begin()) ->arr_t) );
        return curr_time - (*q.begin()) ->arr_t;
   }

   long getTotalWaitResourceTime(long cur_time) const {
       // Sum (wait_time * resource)
       if (q.empty()){return 0;}
       long total =0;
       assert( (cur_time >(*q.begin()) ->arr_t) );
       for(auto tsk:q){
           if(tsk->arr_t < cur_time){
               total+=tsk->resource_req*(cur_time-tsk->arr_t);
           }
       }
       return total;
   }
   long getTotalResourceReq() const{
       long total_req = 0;
       for(auto task_ptr:q){
           total_req+=task_ptr->resource_req;
       }
       return total_req;
   }

    unsigned long numArriveEarlyOrEqualThan(long t){
        // return the number of tasks whose arrival time is less than or equal to a given time t
        // assume new task at end
        unsigned long sum = 0;
        for (const auto t_ptr:q){
            if (t_ptr->arr_t <= t) {++sum;}
            else{break;}
        }
        return sum;
    }
    int getQueueSize() const {return q.size();}

};

struct WaitTaskQueue {
    long num_task_expired;
    long num_task_to_expired;
    long num_demand_expired;
    long num_demand_to_expired;
    long nearest_ddl_in_slot;
    std::multiset<Task *, decltype(comp_wait_q)> ddl_set;

    long num_task_dispatched; // num_task_dispatched in a slot

    // second way use std::function and pass a lambda for constructor
    //std::multiset<Task *, std::function<bool(Task *, Task *)>> ddl_set; //red-black tree
    explicit WaitTaskQueue():
            num_task_expired(0),
            num_task_to_expired(0),
            num_demand_expired{0},
            num_demand_to_expired{0},
            nearest_ddl_in_slot{0},
            ddl_set(comp_wait_q),
            num_task_dispatched{0}
                            {};

    /*bool removeFront() {
        if (ddl_set.empty()) return false;
        ddl_set.erase(ddl_set.begin());
        return true;
    }*/

   /* Task* returnFront() {
        if (ddl_set.empty()) return nullptr;
        return *(ddl_set.begin());
    }*/
   friend std::ostream& operator<<(std::ostream& os, const WaitTaskQueue& q){
       pprint::PrettyPrinter printer(os);
       printer.print("--------DeferTaskQueueStart---------");
       for(auto t_ptr:q.ddl_set){
           os<<*t_ptr;
           //printer.print(*t_ptr);
       }
       print.print("expTask:",q.num_task_expired,"toExpTask",q.num_task_to_expired,
               "numDemandExp",q.num_demand_expired,"numDemandToExpire",q.num_demand_to_expired,
               "QueueSize=",q.ddl_set.size());
       return os;
   }

    void enqueueDefer(Task *tptr) {
        ddl_set.insert(tptr);
    }

    tuple<long,long,long,long> getNumAndDemandDeferTaskExpiredToExpired(long next_slot_time) const{
        long expire_task_cnt = 0;
        long to_expire_task_cnt = 0;
        long expired_total_demand = 0;
        long to_expired_total_demand = 0;

       for(const auto task_ptr:ddl_set){
           if (next_slot_time < task_ptr->start_ddl) {
               break;
           } else if (next_slot_time > task_ptr->start_ddl) {
               ++expire_task_cnt;
               expired_total_demand += task_ptr->resource_req;
           } else if (task_ptr->start_ddl == next_slot_time) {
               ++to_expire_task_cnt;
               to_expired_total_demand += task_ptr->resource_req;
           }
       }
       return {expire_task_cnt,to_expire_task_cnt,
               expired_total_demand,to_expired_total_demand};

   }
    void updateDeferTaskQStatistics(long next_slot_time, int slot_duration) {
        long expire_task_cnt = 0;
        long to_expire_task_cnt = 0;
        long expired_total_demand = 0;
        long to_expired_total_demand = 0;
        for (const auto task_ptr:ddl_set) {
            if (next_slot_time < task_ptr->start_ddl) {
                break;
            } else if (next_slot_time > task_ptr->start_ddl) {
                ++expire_task_cnt;
                expired_total_demand += task_ptr->resource_req;
            } else if (task_ptr->start_ddl == next_slot_time) {
                ++to_expire_task_cnt;
                to_expired_total_demand += task_ptr->resource_req;
            }
        }
        num_task_expired = expire_task_cnt;
        num_task_to_expired = to_expire_task_cnt;
        num_demand_expired = expired_total_demand;
        num_demand_to_expired = to_expired_total_demand;
        if (ddl_set.empty()){
            nearest_ddl_in_slot = CAP_VAL_NEAREST_DDL;
        }else{
            nearest_ddl_in_slot = ((*ddl_set.begin())->start_ddl - next_slot_time) / slot_duration;
        }
    }
    tuple<long,long,long,long> getCurrExpiredTaskInfo(){
        return std::make_tuple(num_task_expired,num_task_to_expired,num_demand_expired,num_demand_to_expired);
    }
    long getQueueSize() const {return ddl_set.size();}
};


#endif //CPP_COMMON_H
