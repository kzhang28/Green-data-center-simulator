//
// Created by Kuo Zhang on 6/14/20.
//

#ifndef CPP_DISPACH_ALGORITHM_H
#define CPP_DISPACH_ALGORITHM_H

#include "common.h"
#include "spdlog/spdlog.h"
#include "spdlog/cfg/env.h"
#include "pprint.h"
#include <iostream>



struct timeline_comp{
    bool operator () (const pair<int,Task*>& l,const pair<int,Task*>& r){
        return l.first < r.first;
    }
};
void printQueue(WaitTaskQueue&wait_task_q,NoneDeferWaitTaskQueue&nondf_wait_task_q ){
    print.print(wait_task_q);print.print(nondf_wait_task_q);}

bool findMatchBinAndAssign( std::map<long,unordered_set<long>>& bin_server_mp,
                            multiset<Task *, decltype(comp_running_q)>& rq,
                            unordered_map<long, Server>& servers,
                            Task* t_ptr,
                            long dispatch_time,
                            spdlog::logger& logger){
    // fill small bin first, then large bins
    for (auto it=bin_server_mp.begin();it!=bin_server_mp.end();++it){
        if(t_ptr->resource_req <= it->first){
            long svr = *((it->second).begin());
            long left_res = it->first -t_ptr->resource_req;
            servers.at(svr).acceptTask(t_ptr,dispatch_time); // this will update task state
            it->second.erase(svr);
            if (it->second.empty()){bin_server_mp.erase(it);}
            if (left_res > 0){bin_server_mp[left_res].insert(svr);}
            rq.insert(t_ptr);
            logger.info("{} {} {} {} {} {}",
                        t_ptr->id,
                        t_ptr->arr_t,
                        t_ptr->dispatch_t,
                        t_ptr->dispatch_attempt_cnt,
                        t_ptr->resource_req,
                        t_ptr->duration);
            return true;
        }
    }
#ifdef DISPATCH_FAILURE
    if (t_ptr->type=="ndf"){
        print.print(*t_ptr,"attempt_time:",dispatch_time);
        print.print(servers);
    }
#endif

    return false;

}

std::map<long,unordered_set<long>> findAllAvailableBins(const unordered_map<long, Server>& servers){
    // find all bins and their corresponding servers: {bin_size : [...server_id...]}
    std::map<long,std::unordered_set<long>> mp;
    for(auto& [name,svr]: servers){
        if (svr.isOn() && svr.getAvailCores()>0){
            mp[svr.getAvailCores()].insert(name);
        }

    }
    return mp;
}

/*long algorithmRemoveEarliestFinishedTask(multiset<Task *, decltype(comp_running_q)>& run_q,
                                        unordered_map<long, std::unique_ptr<Task>>& id_tsk_mp,
                                        unordered_map<long, Server>& servers,
                                        long time_upper_bound) {
    // remove the earliest finished task(s) from running q and server_q and id_task map
    // this task maybe in a server which is in not-ready/unschedulable state
    // return the task finish time or -1 if no such task
    if (!run_q.empty()) {
        long earliest_time = (*run_q.begin())->finish_t; // earliest time a task will finish
        if (earliest_time< time_upper_bound) { // NOTE, the task finishes at next slot time point won't be considered
            for (auto iter=run_q.begin();iter!=run_q.end();){
                if((*iter)->finish_t==earliest_time){
                    long id = (*iter)->id;
                    servers.at((*iter)->assigned_svr).removeTask(*iter);
                    iter = run_q.erase(iter); // must put at last
                    auto num_rm = id_tsk_mp.erase(id); // BUG: deallocation of task happens must at the LAST step
                    assert((num_rm==1));
                }else{
                    break;
                }

            }
            return earliest_time;
        }
    }
    return -1;//
}*/
void removeFinishedTask(multiset<Task *, decltype(comp_running_q)>& run_q,
                        unordered_map<long, std::unique_ptr<Task>>& id_tsk_mp,
                        unordered_map<long, Server>& servers,
                        Task* t_ptr,
                        spdlog::logger& logger){
    /*
     * remove ALL finished task with a given finished time from run_Q, server.taskset, global task_set;
     * NOTE, thumb of two:(1) remove from server (2) remove from run_q  (3) destroy task object (The order Matters)
     * */
    auto [beg,end] = run_q.equal_range(t_ptr);
    for (auto i=beg;i!=end;){
        long id = (*i)->id;
        servers.at((*i)->assigned_svr).removeTask(*i); // must first step
        //logger.info("{} {} {}",(*i)->id,(*i)->arr_t,(*i)->dispatch_t); // must before the next statement
        i = run_q.erase(i); // remove single, must second step
        auto num_rm = id_tsk_mp.erase(id); // deallocation of task happens must at the LAST step
        assert((num_rm==1));
    }

}

map<long,std::set<pair<int,Task*>,timeline_comp>> getEventTimeline(multiset<Task *, decltype(comp_running_q)>& run_q,
                                     WaitTaskQueue& wq,
                                     NoneDeferWaitTaskQueue& nondf_wq,
                                         long next_slot_time,
                                         long curr_slot_time) {
    /*
     * list all events (task finish or task arrival) which may lead to a possible dispatching
     * */
    // {time: {...<event,Task*>}...}, event int coding: 2 -->defer arrival;  1 -->non-defer; 0 -->task finish
    map<long,std::set<pair<int,Task*>,timeline_comp>> timeline_mp;
    for(const auto t_ptr: run_q){
        if (t_ptr->finish_t<next_slot_time){
            assert( (t_ptr->finish_t >= curr_slot_time) );
            timeline_mp[t_ptr->finish_t].insert({0,t_ptr});
        }else{
            break;
        }
    }
    //BUG, there may be task arrival event in wait q, whose arrival time is less than current slot begin,
    // Should skip such event
    for(const auto t_ptr: wq.ddl_set){
        if (t_ptr->arr_t<next_slot_time and t_ptr->arr_t >=curr_slot_time){
            timeline_mp[t_ptr->arr_t].insert({2,t_ptr});
        }
    }
    for(const auto t_ptr:nondf_wq.q){
        if(t_ptr->arr_t < next_slot_time and t_ptr->arr_t >=curr_slot_time ){
            timeline_mp[t_ptr->arr_t].insert({1,t_ptr});
        }
    }
    return timeline_mp;
}


void dispatchNondeferUntilFail(
        unordered_map<long, Server>& servers,
        NoneDeferWaitTaskQueue& nondf_wq,
        multiset<Task *, decltype(comp_running_q)>& rq,
        long trigger_event_time,
        map<long,std::set<pair<int,Task*>,timeline_comp>>& timeline_map,
        long next_slot_time,
        spdlog::logger& logger
        ){
/*     `trigger_event_time`: implication
     (1) the time a running task finishes, or new task arrives, which trigger the possible dispatching,
         so waiting task whose arrival time is greater than this time should be skipped
     (2) This time is also the launch time of the waiting task
     NOTE, this method assumes nondefer queue is [old_task, new_task] (in terms of arrival time)
     Overloaded method used in event trigger loop, after one time dispatch
*/
    auto bin_server_mp = findAllAvailableBins(servers);
    if (bin_server_mp.empty()) {return;}
    for (auto iter =nondf_wq.q.begin();iter!=nondf_wq.q.end(); ){ // Loop all nondefer task
        Task* t_ptr = *iter;
        // nondefer queue: [old,new], so if a task.arr_time > t, all following task.arr_T will be > t
        if (t_ptr->arr_t > trigger_event_time){ break;}
        bool find_match = findMatchBinAndAssign(bin_server_mp, rq, servers, t_ptr, trigger_event_time,logger);
        if (find_match) {
            if (t_ptr->finish_t <next_slot_time){
                timeline_map[t_ptr->finish_t].insert({0,t_ptr});
            }
            nondf_wq.waiting_time.emplace_back(t_ptr->dispatch_t - t_ptr->arr_t);
            iter = nondf_wq.q.erase(iter);
        }else{
            t_ptr->markDispatchFailure(trigger_event_time);
            if(t_ptr->getNumDispatchAttemptFailure() > MAX_NONDF_DISPATCH_FAIL){return;}
            ++iter;}
        if (bin_server_mp.empty()){break;}
    }

}
/*void dispatchNondeferUntilFail(
        unordered_map<long, Server>& servers,
        NoneDeferWaitTaskQueue& nondf_wq,
        multiset<Task *, decltype(comp_running_q)>& rq,
        long trigger_event_time
){
*//*     `trigger_event_time`: implication
     (1) the time a running task finishes, or new task arrives, which trigger the possible dispatching,
         so waiting task whose arrival time is greater than this time should be skipped
     (2) This time is also the launch time of the waiting task
     NOTE, this method assumes nondefer queue is [old_task, new_task] (in terms of arrival time)
*//*
    auto bin_server_mp = findAllAvailableBins(servers);
    if (bin_server_mp.empty()) {return;}
    for (auto iter =nondf_wq.q.begin();iter!=nondf_wq.q.end(); ){ // Loop all nondefer task
        Task* t_ptr = *iter;
        // nondefer queue: [old,new], so if a task.arr_time > t, all following task.arr_T will be > t
        if (t_ptr->arr_t > trigger_event_time){ break;}
        bool find_match = findMatchBinAndAssign(bin_server_mp, rq, servers, t_ptr, trigger_event_time);
        if (find_match) {
            nondf_wq.waiting_time.emplace_back(t_ptr->dispatch_t - t_ptr->arr_t);
            iter = nondf_wq.q.erase(iter);
        }else{
            t_ptr->markDispatchFailure(trigger_event_time);
            if(t_ptr->getNumDispatchAttemptFailure() > MAX_NONDF_DISPATCH_FAIL){return;}
            ++iter;
        }
        if (bin_server_mp.empty()){break;}
    }

}*/

void dispatchDeferBackFillUntilFail(unordered_map<long, Server>& servers,
                           multiset<Task *, decltype(comp_running_q)>& rq,
                           WaitTaskQueue& wq,
                           long trigger_event_time,
                           long num_skipped_task_allowed,
                           long max_failure_allowed,
                           long curr_slot_time,
                                    map<long,std::set<pair<int,Task*>,timeline_comp>>& timeline_map,
                                    long next_slot_time,
                                    spdlog::logger& logger
                           ) {
    /*
     * Algorithm: iterate defer task q from (deadline earliest first), try to find a bin to dispatch
     * Stop backfilling if (1) task_start_ddl <= curr_slot_cnt
     *                     (2) # of dispatch failure attempt > threshold
     *                     (3) # of task_skipped > threshold
     *
     * */
    auto bin_server_mp = findAllAvailableBins(servers);
    if (bin_server_mp.empty()) {return;}
    long task_skipped = 0;
    for (auto iter = wq.ddl_set.begin();iter!=wq.ddl_set.end();){
        Task* t_ptr = *iter;
        if (t_ptr->arr_t > trigger_event_time){
            //BUG,you must move the Iterator;
            ++iter;continue;
        }
        bool find_match = findMatchBinAndAssign(bin_server_mp, rq, servers, t_ptr, trigger_event_time,logger);
        if (find_match){
            ++wq.num_task_dispatched;
            if (t_ptr->finish_t <next_slot_time){
                timeline_map[t_ptr->finish_t].insert({0,t_ptr});
            }
            iter=wq.ddl_set.erase(iter);
        }else{
            (*iter)->markDispatchFailure(trigger_event_time);
            ++task_skipped;
            if ((*iter)->start_ddl<= curr_slot_time){return;}
            if ((*iter)->getNumDispatchAttemptFailure() > max_failure_allowed){return;}
            if (task_skipped > num_skipped_task_allowed){return;}
            ++iter;
        }
        if (bin_server_mp.empty()){break;}

    }
}

/*void dispatchDeferBackFillUntilFail(unordered_map<long, Server>& servers,
                                    multiset<Task *, decltype(comp_running_q)>& rq,
                                    WaitTaskQueue& wq,
                                    long trigger_event_time,
                                    long num_skipped_task_allowed,
                                    long max_failure_allowed,
                                    long curr_slot_time
) {
    *//*
     *  Overloaded function
     * Algorithm: iterate defer task q from (deadline earliest first), try to find a bin to dispatch
     * Stop backfilling if (1) task_start_ddl <= curr_slot_cnt
     *                     (2) # of dispatch failure attempt > threshold
     *                     (3) # of task_skipped > threshold
     *
     * *//*
    auto bin_server_mp = findAllAvailableBins(servers);
    if (bin_server_mp.empty()) {return;}
    long task_skipped = 0;
    for (auto iter = wq.ddl_set.begin();iter!=wq.ddl_set.end();){
        Task* t_ptr = *iter;
        if (t_ptr->arr_t > trigger_event_time){
            //BUG,there is no need to consider a task that arrives after task finished time, however,
            // you must move the Iterator;
            ++iter;continue;
        }
        bool find_match = findMatchBinAndAssign(bin_server_mp, rq, servers, t_ptr, trigger_event_time);
        if (find_match){
            ++wq.num_task_dispatched;
            iter=wq.ddl_set.erase(iter);
        }else{
            (*iter)->markDispatchFailure(trigger_event_time);
            ++task_skipped;
            if ((*iter)->start_ddl<= curr_slot_time){return;}
            if ((*iter)->getNumDispatchAttemptFailure() > max_failure_allowed){return;}
            if (task_skipped > num_skipped_task_allowed){return;}
            ++iter;
        }
        if (bin_server_mp.empty()){break;}

    }
}*/
void dispatchDeferTaskTriggeredByArrival(unordered_map<long, Server>& servers,
                                    multiset<Task *, decltype(comp_running_q)>& rq,
                                    WaitTaskQueue& wq,
                                    long arrival_time,
                                    long curr_slot_time,
                                    long num_skipped_task_allowed,
                                         long max_failure_allowed,
                                         map<long,std::set<pair<int,Task*>,timeline_comp>>& timeline_map,
                                         long next_slot_time,
                                         spdlog::logger& logger
) {
    /*
     *  consider whether any task whose arrival time is `arrival_time` can be dispatched
     *  implication: whether a task that arrives before the given time can be dispatched won't be affected
     *  NOTE, Dispatch the new arrival task ONLY when there is no head of line block, so it is possible that the
     *      new arrival task will not be dispatched even though the resources are enough for it.
     * */
    auto bin_server_mp = findAllAvailableBins(servers);
    if (bin_server_mp.empty()) {return;}
    long task_skipped = 0;
    for (auto iter = wq.ddl_set.begin();iter!=wq.ddl_set.end();){
        Task* t_ptr = *iter;
        if (t_ptr->arr_t > arrival_time){
            ++iter;continue;
        }
        bool find_match = findMatchBinAndAssign(bin_server_mp,rq,servers,t_ptr,arrival_time,logger);
        if (find_match){
            ++wq.num_task_dispatched;
            if (t_ptr->finish_t <next_slot_time){
                timeline_map[t_ptr->finish_t].insert({0,t_ptr});
            }
            iter=wq.ddl_set.erase(iter);
        }else{
            ++task_skipped;
            // there are more important tasks blocked which avoids dispatching the just arrival task
            if ((*iter)->start_ddl<= curr_slot_time){return;}
            if ((*iter)->getNumDispatchAttemptFailure() > max_failure_allowed){return;}
            if (task_skipped > num_skipped_task_allowed){return;}
            ++iter;
        }
        if (bin_server_mp.empty()){break;}

    }
}

/*void algorithmLoopFindFitWaitingTaskAndAssign(WaitTaskQueue& wq,
                                              NoneDeferWaitTaskQueue& nondf_wq,
                                              multiset<Task *, decltype(comp_running_q)>& rq,
                                              unordered_map<long, Server>& servers,
                                              unordered_map<long, std::unique_ptr<Task>>& id_tsk_mp,
                                              long next_slot_time,
                                              long num_block_task_allowed,
                                              long max_failure_allowed,
                                              long curr_slot_time){
    *//*
     * keep attempting dispatch tasks as older tasks finish, mimic the time elapse until next slot
     * loop stop condition: no finished task (no new bins); there are bins but no fitted task(bins are too small)
        the task being removed must guarantee that task.finish_time < `next_slot_time`
        finish_t may be == current_slot_time
     * *//*

    long finish_t = algorithmRemoveEarliestFinishedTask(rq, id_tsk_mp, servers, next_slot_time);

    while(finish_t!=-1){
        //spdlog::info("At least one task have been removed from Run queue");
        dispatchNondeferUntilFail(servers,nondf_wq,rq,finish_t); // return, if non-defer q is empty
        // still have nondefer task (arrival before finish_t) that has not been dispatched
        if (nondf_wq.numArriveEarlyOrEqualThan(finish_t) > 0 ){ // still have nondefer task that has not been dispatched
            finish_t = algorithmRemoveEarliestFinishedTask(rq, id_tsk_mp, servers, next_slot_time);
            continue;
        }
        dispatchDeferBackFillUntilFail(servers,rq,wq,finish_t,num_block_task_allowed,
                                        max_failure_allowed,curr_slot_time);
        finish_t = algorithmRemoveEarliestFinishedTask(rq, id_tsk_mp, servers, next_slot_time);
    }
    //BUG, at this point, tasks that arrives during (last_finish_t,next_slot_time) will not be dispatched
    // Perform the last attempt:
    dispatchNondeferUntilFail(servers,nondf_wq,rq,next_slot_time,true);
    if (nondf_wq.numArriveEarlyOrEqualThan(next_slot_time) == 0 ){ // no nondefer task left
        dispatchDeferBackFillUntilFail(servers,rq,wq,num_block_task_allowed,max_failure_allowed,
                curr_slot_time,next_slot_time,true);
    }
}*/
/*void eventTriggerDispatch(WaitTaskQueue& wq,
                                              NoneDeferWaitTaskQueue& nondf_wq,
                                              multiset<Task *, decltype(comp_running_q)>& rq,
                                              unordered_map<long, Server>& servers,
                                              unordered_map<long, std::unique_ptr<Task>>& id_tsk_mp,
                                              long next_slot_time,
                                              long num_block_task_allowed,
                                              long max_failure_allowed,
                                              long curr_slot_time,
                          spdlog::logger& logger){
    *//*
     * what triggers a possible dispatch:
     *         (1) new task arrival, (2) older task finishes (3) slot resource adjustment (separately handled)
     * *//*
    map<long,std::set<pair<int,Task*>,timeline_comp>> timeline_mp = getEventTimeline(rq,wq,nondf_wq,
            next_slot_time,curr_slot_time);
#ifdef DEBUG_DISPATCH
    spdlog::info("getEventTimeLine Done");
    print.print(timeline_mp);
#endif
    while(!timeline_mp.empty()){
        long time = timeline_mp.begin()->first;
        std::set<pair<int,Task*>,timeline_comp> set = timeline_mp.begin()->second;
#ifdef DEBUG_DISPATCH
        spdlog::info("An event triggered process begins at {}",time);
#endif
        if (set.begin()->first==0){
            //spdlog::info("task finish time event at {}",time);
            // there are tasks that finishes at this time
            removeFinishedTask(rq,id_tsk_mp,servers,set.begin()->second,logger);
            //spdlog::info("      -->remove finished task done");
            dispatchNondeferUntilFail(servers,nondf_wq,rq,time,timeline_mp,next_slot_time);
            //spdlog::info("      -->nondefer dispatch done");
            if (nondf_wq.numArriveEarlyOrEqualThan(time) == 0 ){
                dispatchDeferBackFillUntilFail(servers,rq,wq,time,num_block_task_allowed,max_failure_allowed,
                        curr_slot_time,timeline_mp,next_slot_time);
                //spdlog::info("      -->defer dispatch done");
            }
        }else if (set.begin()->first==1 and set.rbegin()->first==1){
            // there are NO tasks that finishes at this time
            // only non-defer arrival happens at this time point
            //spdlog::info("only non-defer arrival time event at {}",time);
            dispatchNondeferUntilFail(servers,nondf_wq,rq,time,timeline_mp,next_slot_time);
        }else if (set.begin()->first==2 and set.rbegin()->first==2){
            // there are NO tasks that finishes at this time
            // only defer arrival happens at this time point
            //spdlog::info("only defer arrival time event at {}",time);
            if (nondf_wq.numArriveEarlyOrEqualThan(time) == 0 ){
                dispatchDeferTaskTriggeredByArrival(servers,rq,wq,time,curr_slot_time,num_block_task_allowed,
                        max_failure_allowed,timeline_mp,next_slot_time);
            }
        }else if(set.begin()->first==1 and set.rbegin()->first==2){
            // both defer and non-defer have arrivals
            //spdlog::info("both nondefer and  defer arrival time event at {}",time);
            dispatchNondeferUntilFail(servers,nondf_wq,rq,time,timeline_mp,next_slot_time);
            if (nondf_wq.numArriveEarlyOrEqualThan(time) == 0 ){
                dispatchDeferTaskTriggeredByArrival(servers,rq,wq,time,curr_slot_time,num_block_task_allowed,
                        max_failure_allowed,timeline_mp,next_slot_time);
            }
        }else{
            assert(false);
        }
        timeline_mp.erase(timeline_mp.begin());
#ifdef DEBUG_DISPATCH
        print.print("Servers:\n",servers);
#endif
        //printQueue(wq,nondf_wq);
    }
}*/
void eventTriggerDispatch_2(WaitTaskQueue& wq,
                          NoneDeferWaitTaskQueue& nondf_wq,
                          multiset<Task *, decltype(comp_running_q)>& rq,
                          unordered_map<long, Server>& servers,
                          unordered_map<long, std::unique_ptr<Task>>& id_tsk_mp,
                          long next_slot_time,
                          long num_block_task_allowed,
                          long max_failure_allowed,
                          long curr_slot_time,
                          spdlog::logger& logger,
                          vector<long>& wkup_vec,
                          long ave_wkup_duration){
    /*
     * what triggers a possible dispatch:(1) new task arrival, (2) older task finishes (3) server wake-up
     * event int coding: 3-->wakeup; 2 -->defer arrival;  1 -->non-defer; 0 -->task finish
     * */
    map<long,std::set<pair<int,Task*>,timeline_comp>> timeline_mp = getEventTimeline(rq,wq,nondf_wq,
                                                                                     next_slot_time,curr_slot_time);
    // adding server wake up events
    if (!wkup_vec.empty()){
        // NOTE, assume all svrs take the same time to wake-up, only one record is enough
        timeline_mp[curr_slot_time+ave_wkup_duration].insert({3, nullptr});
    }
    while(!timeline_mp.empty()){
        long time = timeline_mp.begin()->first;
        std::set<pair<int,Task*>,timeline_comp> curr_set = timeline_mp.begin()->second;

        if (curr_set.begin()->first == 0 || curr_set.rbegin()->first==3){
            // there are tasks that finishes or svr wake-up  at this time
            if (curr_set.begin()->first==0){// remove all tasks which finish at this time
                removeFinishedTask(rq, id_tsk_mp, servers, curr_set.begin()->second, logger);
            }
            if (curr_set.rbegin()->first==3){ // mark servers on from wake-up
                for(auto svr:wkup_vec){
                    // NOTE, assume all svrs take the same time to wake-up, only one record is enough
                    servers.at(svr).setOnFromWakeUp();
                }
            }
            //spdlog::info("      -->remove finished task done");
            dispatchNondeferUntilFail(servers,nondf_wq,rq,time,timeline_mp,next_slot_time,logger);
            //spdlog::info("      -->nondefer dispatch done");
            if (nondf_wq.numArriveEarlyOrEqualThan(time) == 0 ){
                dispatchDeferBackFillUntilFail(servers,rq,wq,time,num_block_task_allowed,max_failure_allowed,
                                               curr_slot_time,timeline_mp,next_slot_time,logger);
                //spdlog::info("      -->defer dispatch done");
            }
        }else if (curr_set.begin()->first == 1 and curr_set.rbegin()->first == 1){
            // there are NO tasks that finishes at this time, only non-defer arrival happens at this time point
            //spdlog::info("only non-defer arrival time event at {}",time);
            dispatchNondeferUntilFail(servers,nondf_wq,rq,time,timeline_mp,next_slot_time,logger);
        }else if (curr_set.begin()->first == 2 and curr_set.rbegin()->first == 2){
            // there are NO tasks that finishes at this time
            // only defer arrival happens at this time point
            //spdlog::info("only defer arrival time event at {}",time);
            if (nondf_wq.numArriveEarlyOrEqualThan(time) == 0 ){
                dispatchDeferTaskTriggeredByArrival(servers,rq,wq,time,curr_slot_time,num_block_task_allowed,
                                                    max_failure_allowed,timeline_mp,next_slot_time,logger);
            }
        }else if(curr_set.begin()->first == 1 and curr_set.rbegin()->first == 2){
            // both defer and non-defer have arrivals
            //spdlog::info("both nondefer and  defer arrival time event at {}",time);
            dispatchNondeferUntilFail(servers,nondf_wq,rq,time,timeline_mp,next_slot_time,logger);
            if (nondf_wq.numArriveEarlyOrEqualThan(time) == 0 ){
                dispatchDeferTaskTriggeredByArrival(servers,rq,wq,time,curr_slot_time,num_block_task_allowed,
                                                    max_failure_allowed,timeline_mp,next_slot_time,logger);
            }
        }else{
            assert(false);
        }
        timeline_mp.erase(timeline_mp.begin());
        //printQueue(wq,nondf_wq);
    }
}

#endif //CPP_DISPACH_ALGORITHM_H
