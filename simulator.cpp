//
// Created by Kuo Zhang on 6/6/20.
// Done is better than perfect !!!
//

#include "common.h"
#include "power_cooling.h"
#include "dispach_algorithm.h"
#include "reward.h"
#include <numeric>
#include <cstdint>
#include <iostream>

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include "spdlog/spdlog.h"
#include "spdlog/cfg/env.h"
#include "pprint.h"


namespace py = pybind11;

pprint::PrettyPrinter print;
class Cluster {
public:
    explicit Cluster(int num_server,
            int slot_dur, // in second
            long curr_slot_time,
            int num_block_task_allow,
            double temp_in_max,
            double temp_in_min,
            double temp_pen_coeff,
            double backlog_pen_coeff,
            double backog_nondf_pen_coeff,
            double green_usage_reward_coeff,
            double pue_cap_max,
            double pue_cap_min,
            int max_dispatch_failure_allowed,
            std::string reward_f,
            std::string log_level,
            std::string logger_name,
            std::string ac_cooling_model,
            std::vector<std::tuple<int, std::string, int>> server_specs // <name,type,total cores>

    ) : num_server(num_server),
        slot_duration(slot_dur),
        num_block_allowed(num_block_task_allow),
        max_dispatch_failure_allowed(max_dispatch_failure_allowed),
        curr_slot_time(curr_slot_time),
        next_slot_time(slot_dur + curr_slot_time),
        ac_cool_model(ac_cooling_model),
        all_sleep_server_lst(),
        wait_task_q(),
        nondf_wait_task_q(),
        run_q(comp_running_q),
        env{25,0,0,
            0,0,0,0,0
            ,0,0,0},
            reward_obj(temp_in_max, temp_in_min, temp_pen_coeff,
                    backlog_pen_coeff,backog_nondf_pen_coeff,green_usage_reward_coeff,
                    pue_cap_max, pue_cap_min,reward_f),
                    NUM_CORES_WKUP(0),
                    AVE_WAKEUP_DURATION(15){
        spdlog::shutdown(); //clear logger
        if (logger_name=="console"){
            this->logger = spdlog::stdout_color_mt("console");
        }else{
            // NOTE,default is append
            this->logger = spdlog::basic_logger_mt("file_logger", "./runtime_log/"+logger_name);
            logger->set_pattern("%v");
        }
        if (log_level=="warn"){
            logger->set_level(spdlog::level::warn);
        }

        long total_avail_re = 0;
        long cores;
        for (auto &tp:server_specs) {
            long name(std::get<0>(tp));
            cores = std::get<2>(tp);
            Server svr(name, std::get<1>(tp), cores, 1);
            servers.insert(std::make_pair(name, svr));
            all_alive_server_lst.push_back(name);
            total_avail_re += cores;
        }
        //print.print("numServer:",num_server,"pueCapMax:",pue_cap_max);
        total_avail_res = total_avail_re;
        //print.print("Servers:",servers);
        assert( (servers.size()==num_server) );
    };


    void logAllServerState(){
        std::ostringstream oss;
        oss<<"serverState#";
        for(long i=0;i<num_server;++i){
            oss<<i<<" "<<servers.at(i).getState()<<"#";
        }
//        for(auto& p:servers){
//            oss<<p.first<<" "<<p.second.getState()<<"#";
//        }
        logger->info(oss.str());
    }
    std::tuple<vector<long>, vector<pair<long, long>>> selectServersToWkupSleepAndUpdate(long alloc,
                                                                                      vector<long> &svr_cost_energy) {
        //<wkupVec,<sleepSvrID,timeTaskFinish>>
        //NOTe, this method updates server states
        //                   front         front
        //  ----------[xd,xd ]--------------[xd xd xe xe]-------
        //              \----sleeplist         \-----aliveList
        // Note: the selection of server to sleep is different than real-implementation (no sorting
        // based on # of task left)

        svr_cost_energy.insert(svr_cost_energy.begin(), all_alive_server_lst.begin(),
                all_alive_server_lst.end());
        long avail_resource = getCurrTotalAvailRes(); //total cores of on servers
        if (alloc > avail_resource) { // wake up
            vector<long> res;
            long diff = alloc - avail_resource;
            while (diff > 0 && !all_sleep_server_lst.empty()) {
                auto svr = all_sleep_server_lst.front();
                res.push_back(svr);
                all_alive_server_lst.push_front(svr);
                all_sleep_server_lst.pop_front();
                servers.at(svr).setOn();
                svr_cost_energy.push_back(svr); // new alive servers that needs get considered for energy cost
                diff -= servers.at(svr).getTotalCores();
            }
            return {res, {}};
        } else if (alloc < avail_resource) { // sleep servers
            vector<pair<long, long>> res_tuple_vec; //to sleep server and the last task finish time
            long diff = avail_resource - alloc;
            while (diff >=servers.at(all_alive_server_lst.front()).getTotalCores() //need enough redundancy to suspend a server
                   && all_alive_server_lst.size() > 1) { //always keep one server alive
                long svr = all_alive_server_lst.front(); //svr to be sleep
                // since there is no task will be allocated in the server, the last finished task time could be resolved
                res_tuple_vec.emplace_back(svr, servers.at(svr).getLastFinishedTaskTime(id_tsk_mp, curr_slot_time));
                all_sleep_server_lst.push_front(svr);
                all_alive_server_lst.pop_front();
                servers.at(svr).setUnschedulable(); // on but not ready
                diff -= servers.at(svr).getTotalCores();
            }
            return {{}, res_tuple_vec};
        }
        return {{},
                {}};
    }
    std::tuple<vector<long>, vector<pair<long, long>>> selectServersToWkupSleepAndUpdate_2(long alloc,
                                                                                         vector<long> &svr_cost_energy) {
        //<wkupVec,<sleepSvrID,timeTaskFinish>>
        //NOTe, this method updates server states
        //                   front         front
        //  ----------[xd,xd ]--------------[xd xd xe xe]-------
        //              \----sleeplist         \-----aliveList
        // Note: the selection of server to sleep is different than real-implementation (no sorting
        // based on # of task left)

        svr_cost_energy.insert(svr_cost_energy.begin(), all_alive_server_lst.begin(),
                               all_alive_server_lst.end());
        long avail_resource = getCurrTotalAvailRes(); //total cores of on servers
        if (alloc > avail_resource) { // wake up
            vector<long> res;
            long diff = alloc - avail_resource;
            while (diff > 0 && !all_sleep_server_lst.empty()) {
                auto svr = all_sleep_server_lst.front();
                res.push_back(svr);
                all_alive_server_lst.push_front(svr);
                all_sleep_server_lst.pop_front();
                servers.at(svr).markWakingUp(NUM_CORES_WKUP);
                svr_cost_energy.push_back(svr); // new alive servers that needs get considered for energy cost
                diff -= servers.at(svr).getTotalCores();
            }
            return {res, {}};
        } else if (alloc < avail_resource) { // sleep servers
            vector<pair<long, long>> res_tuple_vec; //to sleep server and the last task finish time
            long diff = avail_resource - alloc;
            while (diff >=servers.at(all_alive_server_lst.front()).getTotalCores() //need enough redundancy to suspend a server
                   && all_alive_server_lst.size() > 1) { //always keep one server alive
                long svr = all_alive_server_lst.front(); //svr to be sleep
                // since there is no task will be allocated in the server, the last finished task time could be resolved
                res_tuple_vec.emplace_back(svr, servers.at(svr).getLastFinishedTaskTime(id_tsk_mp, curr_slot_time));
                all_sleep_server_lst.push_front(svr);
                all_alive_server_lst.pop_front();
                servers.at(svr).setUnschedulable(); // on but not ready
                diff -= servers.at(svr).getTotalCores();
            }
            return {{}, res_tuple_vec};
        }
        return {{},
                {}};
    }

    void enqueue(long id, int duration, int req, long start_ddl, long arr_t, string type) {
                     // id,duration,resource_req,start_ddl,arr_t
        // enqueue wait q and ownship q
        const auto[it, success] = id_tsk_mp.insert(
                make_pair(id, std::make_unique<Task>(id, type, duration, req, arr_t, start_ddl)));
        if (success) {
            if (type=="df"){
                wait_task_q.enqueueDefer(it->second.get());
#ifdef ENQUEUE_INFO
                spdlog::info("Defer Task enqueue succeeds with id: {}", id);
#endif
            }else{
                nondf_wait_task_q.enqueueNonDf(it->second.get());
#ifdef ENQUEUE_INFO
                spdlog::info("NonDefer Task enqueue succeeds with id: {}", id);
#endif
            }
        } else {
            spdlog::error("Task enqueue fails with id: {}", id);
        }

    }

// ############################################### Core Output Function ####################################
    std::tuple<long, long,long, long, long,long, double,double,long,long,long,long,long,double> getClusterState() {
        // this function will be called after step but before next step
        return make_tuple(wait_task_q.num_task_expired,
                          wait_task_q.num_task_to_expired,
                          wait_task_q.num_demand_expired,
                          wait_task_q.num_demand_to_expired,
                          wait_task_q.getQueueSize(),
                          nondf_wait_task_q.getQueueSize(),
                          env.next_temp,
                          env.cluster_utilization, //cluster-wide
                          wait_task_q.nearest_ddl_in_slot,
                          env.num_df_dispatched,
                          env.num_nondf_dispatched,
                          env.total_cores_current_on,
                          nondf_wait_task_q.getOldestTaskArrivalTimeDistance(next_slot_time),
                          env.nondf_wait_t_total  //
                          );
    }
// ############################################### Core function ####################################
   /* std::unordered_map<string, double> step(int alloc, bool ac, int fan,
                                            double green_energy,
                                            double in_temp,
                                            double out_temp,
                                            int prev_fan,
                                            double prev_in_temp,
                                            double prev_out_temp,
                                            long curr_slot_t) {
        logger->info("slot {}",curr_slot_t);
        std::ostringstream oss;
        this->curr_slot_time = curr_slot_t;
        this->next_slot_time = curr_slot_t + slot_duration;
        // all action should have been capped prior to the invocation
        double cooling_energy = Power::getCoolingEnergyReal(fan, ac, slot_duration);
        // servers which will be consuming energy, will be filled up in the following method
        vector<long> svrs_cost_energy;
        auto[wake_vec, sleep_tuple_vec] = selectServersToWkupSleepAndUpdate(alloc, svrs_cost_energy);
        logAllServerState();
        for (auto svr:svrs_cost_energy){
            oss<<svr<<" ";
        }
        logger->info(oss.str());
#ifdef DEBUG_ALG
        spdlog::warn("%%%%%%%%%%%-------------------SLOT_Time:{}--------------------------%%%%%%%%%%%%%%%",curr_slot_time);
        print.compact(true);
        print.print("wake_vec:",wake_vec,"sleepTupleVec:",sleep_tuple_vec);
        print.print("svrCostEnergy:",svrs_cost_energy);
        print.compact(false);
        printAliveSleepServers();
        printQueue();
#endif
        resetServerUtilStatistics(curr_slot_time);
#ifdef DEBUG_ALG
        print.print("--------Before Dispatch:---------");
        printServers();
#endif
        dispatch(num_block_allowed,max_dispatch_failure_allowed,curr_slot_time);
#ifdef DEBUG_ALG
        print.print("---------After Dispatch:----------");
        printServers();
        printQueue();
        printRunQ();
#endif
        long total_suspend_time = 0;
        if (!sleep_tuple_vec.empty()) {
            total_suspend_time = rollbackServerState(sleep_tuple_vec);
        }
        logAllServerState();
#ifdef DEBUG_ALG
        print.print("---------After Rollback:----------");
        printServers();
        printAliveSleepServers();
#endif
        auto server_utilization_vec = getEachAverageServerUtil(svrs_cost_energy, next_slot_time);
        oss.str(std::string());
        oss.clear();
        for(auto utili:server_utilization_vec){
            oss<<utili<<" ";
        }
        logger->info(oss.str());
#ifdef DEBUG_ALG
        print.print("ServerUtilization:",server_utilization_vec);
#endif
        double totalServerEnergy = Power::getTotalServerEnergyCost(server_utilization_vec, slot_duration,
                                                                    total_suspend_time);
        // cluster-wide server utilization
        double clusterUtilization = computeClusterUtilization(svrs_cost_energy, server_utilization_vec);
        double next_temp = Cooling::deriveNextTempRealWithAc(fan, ac, out_temp, in_temp, clusterUtilization,
                                                                prev_fan,
                                                             prev_in_temp, prev_out_temp,ac_cool_model);
        env.next_temp = next_temp;
        env.cluster_utilization = clusterUtilization;
        //wait_task_q.updateDeferTaskQStatistics(next_slot_time);
#ifdef DEBUG_ALG
        spdlog::info("After update cluster state");
        printQueue();
#endif
        // auto [num_exp, num_to_exp, num_demand_exp, num_demand_to_exp] = wait_task_q.getCurrExpiredTaskInfo();
//        unordered_map<string, double> reward_dict = reward_obj.reward_1_1(totalServerEnergy,
//                                                                          green_energy, num_exp, next_temp,
//                                                                          cooling_energy);
        updateClusterStateAndComputeStatistics(next_slot_time);
        unordered_map<string, double> reward_dict = reward_obj.reward_func_obj(totalServerEnergy,
                                                                          green_energy,
                                                                          wait_task_q.num_task_expired,
                                                                          next_temp,
                                                                          cooling_energy,
                                                                          nondf_wait_task_q.getQueueSize());
        return reward_dict;
    }*/
// ############################################### Core function Done ####################################
    std::unordered_map<string, double> step(int alloc,
                                              bool ac,
                                              int fan,
                                            double green_energy,
                                            double in_temp,
                                            double out_temp,
                                            int prev_fan,
                                            double prev_in_temp,
                                            double prev_out_temp,
                                            long curr_slot_t) {
        // NOTE,difference than previous version,
        //  --> adding wake-up cost and delay into consideration, this version is to replace the previous one,
        //  --> change reward function: using total nondefer waiting time as the factor for penalty computation
        logger->info("slot {}",curr_slot_t);
        std::ostringstream oss;
        this->curr_slot_time = curr_slot_t;
        this->next_slot_time = curr_slot_t + slot_duration;
        // all action should have been capped prior to the invocation
        double cooling_energy = Power::getCoolingEnergyReal(fan, ac, slot_duration);
        // servers which will be consuming energy, will be filled up in the following method
        vector<long> svrs_cost_energy;
        auto[wake_vec, sleep_tuple_vec] = selectServersToWkupSleepAndUpdate_2(alloc, svrs_cost_energy);
        logAllServerState();
        for (auto svr:svrs_cost_energy){
            oss<<svr<<" ";
        }
        logger->info(oss.str());
        resetServerUtilStatistics(curr_slot_time);
        dispatch_2(num_block_allowed,max_dispatch_failure_allowed,curr_slot_time,wake_vec);
        long total_suspend_time = 0;
        if (!sleep_tuple_vec.empty()) {
            total_suspend_time = rollbackServerState(sleep_tuple_vec);
        }
        logAllServerState();
        auto server_utilization_vec = getEachAverageServerUtil(svrs_cost_energy, next_slot_time);
        oss.str(std::string());
        oss.clear();
        for(auto utili:server_utilization_vec){
            oss<<utili<<" ";
        }
        logger->info(oss.str());
        double totalServerEnergy = Power::getTotalServerEnergyCost(server_utilization_vec, slot_duration,
                                                                   total_suspend_time);
        // cluster-wide server utilization during last slot
        double clusterUtilization = computeClusterUtilization(svrs_cost_energy, server_utilization_vec);
        double next_temp = Cooling::deriveNextTempRealWithAc(fan, ac, out_temp, in_temp, clusterUtilization,
                                                             prev_fan,
                                                             prev_in_temp, prev_out_temp,ac_cool_model);
        env.next_temp = next_temp;
        env.cluster_utilization = clusterUtilization;
        updateClusterStateAndComputeStatisticsContinuous(next_slot_time);
        unordered_map<string, double> reward_dict = reward_obj.reward_func_obj_2(totalServerEnergy,
                                                                               green_energy,
                                                                               wait_task_q.num_task_expired,
                                                                               next_temp,
                                                                               cooling_energy,
                                                                               env.nondf_wait_t_cnt, //NOTE,changed
                                                                               env.nondf_wait_t_total);//NOTE, added
        return reward_dict;
    }

    void printServers(){print.print("Servers:\n",servers);}
    void printQueue(){print.print(wait_task_q);print.print(nondf_wait_task_q);}
    void printAliveSleepServers(){
        print.compact(true);
        print.line_terminator("");
        print.print(all_alive_server_lst,"<-alive|sleep->",all_sleep_server_lst,"\n");
        print.line_terminator("\n");
    }
    void printRunQ(){
        print.print("[--------********* RunQ *********-------]");
        print.line_terminator("");
        for(auto t_ptr:run_q){
            print.print(*t_ptr);
        }
        print.line_terminator("\n");
    }

private:
    const int num_server;
    const int slot_duration; //duration in seconds
    const long num_block_allowed; // how many tasks can be skipped when try to dispatching defer task
    const long max_dispatch_failure_allowed;
    long curr_slot_time;
    long next_slot_time;

    long total_avail_res;
    string ac_cool_model;

    //---------------------------------
    unordered_map<long, Server> servers; // name->server
    std::deque<long> all_alive_server_lst;
    std::deque<long> all_sleep_server_lst;
    WaitTaskQueue wait_task_q;
    NoneDeferWaitTaskQueue nondf_wait_task_q;
    //RunningTaskQueue run_q;
    std::multiset<Task *, decltype(comp_running_q)> run_q;
    unordered_map<long, std::unique_ptr<Task>> id_tsk_mp; // unique pointer,for auto-destroy task object
    Env env;
    Reward reward_obj;
    std::shared_ptr<spdlog::logger> logger;
    const long NUM_CORES_WKUP;
    const long AVE_WAKEUP_DURATION;

    void resetServerUtilStatistics(long curr_time) {
        for (auto &[name, svr]: servers) {
            if (!svr.isOff()) {
                svr.resetCumuUtil(curr_time);
            }
        }
    }

    long getCurrTotalAvailRes() const {
        long avail_resource = 0;
        for (const auto&[name, server]:servers) {
            assert((!server.isUnschedulable()));
            if (server.isOn()) {
                avail_resource += server.getTotalCores();
            }
        }
        return avail_resource;
    }

    /*void dispatch(long num_block_task_allow, long max_failure_allowed,long curr_slot_time) {
        *//* (1) A task arrives at time t is ok to be dispatched at time t.
         * (2) The reason why at the beginning there is a Separate dispatch attempt is:
         *   if they were eliminated, a task that arrives at exactly "curr_slot_time" may not be dispatched immediately.
         *   (each new slot allocate or de-allocate resources can cause dispatch event)
         * (3) Always only when non-defer tasks have been dispatched, can defer tasks be dispatched.
         * *//*
        // one time dispatch:
        assert(( run_q.empty() || (*run_q.begin())->finish_t >= curr_slot_time));
        // NOTE, it does not matter this one-time dispatch does not remove finished task, b/c the eventTrigger method
        //      will remove the finished taske (finished exactly at curr_slot_time) and dispatch it if possible;
        dispatchNondeferUntilFail(servers, nondf_wait_task_q, run_q, curr_slot_time);
#ifdef DEBUG_DISPATCH
        spdlog::info("[After One time noneDefer dispatch]");
        printRunQ();
        printServers();
#endif
        if (nondf_wait_task_q.numArriveEarlyOrEqualThan(curr_slot_time) == 0) {
            dispatchDeferBackFillUntilFail(servers, run_q, wait_task_q, curr_slot_time,
                    num_block_task_allow,max_failure_allowed,curr_slot_time);
#ifdef DEBUG_DISPATCH
            spdlog::info("[After one time Defer dispatch]");
            printRunQ();
            printServers();
#endif
        }
        eventTriggerDispatch(wait_task_q,nondf_wait_task_q,run_q,servers,id_tsk_mp,next_slot_time,
                num_block_task_allow,max_failure_allowed,curr_slot_time,*logger);
    }*/
    void dispatch_2(long num_block_task_allow, long max_failure_allowed,long curr_slot_time,vector<long>& wkup_lst) {
        /* (1) A task arrives at time t is ok to be dispatched at time t.
         * (2) The reason why at the beginning there is a Separate dispatch attempt is:
         *      each new slot allocate or de-allocate resources can cause dispatch event
         * (3) Always only when non-defer tasks have been dispatched, can defer tasks be dispatched.
         * */
        assert(( run_q.empty() || (*run_q.begin())->finish_t >= curr_slot_time));
        eventTriggerDispatch_2(wait_task_q,nondf_wait_task_q,run_q,servers,id_tsk_mp,next_slot_time,
                             num_block_task_allow,max_failure_allowed,curr_slot_time,*logger,
                             wkup_lst,
                             AVE_WAKEUP_DURATION);
    }

    long rollbackServerState(vector<pair<long, long>> &sleep_tuple_vec) {
        //possibly roll-back svr from sleep list,
        // make sure the state of a server is either On or Off at each slot beginning
        // when the last task finished in the unschedulable server, it will go to sleep; this value will be used to
        // revise the server energy consumption. (the current server power consumption computation method
        // does not account for the suspended state)
        long total_sleep_time = 0;
        auto iter = all_sleep_server_lst.begin(); // front is the last in; sleep_tuple_vec: back is last in
        for (int i = sleep_tuple_vec.size() - 1; i >= 0; i--) { //NOTE, careful when vector.size()-xxx
            assert((*iter == sleep_tuple_vec[i].first));
            if (sleep_tuple_vec[i].second >= next_slot_time) { // running until next slot
                servers.at(*iter).setOn(); // set on
                all_alive_server_lst.push_front(*iter);
                iter = all_sleep_server_lst.erase(iter);
            } else {
                total_sleep_time+= (next_slot_time - sleep_tuple_vec[i].second); //+how long this server been suspended
                servers.at(*iter).setOff(); // set off
                ++iter;
            }
        }
        return total_sleep_time;
    }

    vector<double> getEachAverageServerUtil(const vector<long> &svr_id_vec, long until_time) {
        // get average utilization between the latest reset time and the this method's invocation time (until_time)
        vector<double> res;
        for (const auto svr_id: svr_id_vec) {
            res.push_back(servers.at(svr_id).getCumuUtilizationUntil(until_time));
        }
        return res;
    }

    double computeClusterUtilization(vector<long> &servers_up, vector<double> &ave_utilizations) const {
        double num_up_svr = servers_up.size(); // up servers during the slot
        return (num_up_svr / num_server) * (std::accumulate(
                ave_utilizations.begin(), ave_utilizations.end(), 0.0) / ave_utilizations.size());
    }
    long getTotalCoresCurrentOn() const{
        long total = 0;
        for(auto& [id,svr] : servers){
            if(svr.isOn()){
                total+=svr.getTotalCores();
            }
        }
        return total;
    }

    void updateClusterStateAndComputeStatisticsContinuous(long next_slot_time) {
        //env.instant_on_server_utilization = getClusterInstantUtilizationForOnServer();
        env.total_cores_current_on = getTotalCoresCurrentOn();
        wait_task_q.updateDeferTaskQStatistics(next_slot_time, slot_duration);
        auto [total_wt,cnt,max_wt,num_dispatched] = nondf_wait_task_q.getToalWaitingTimeIncludeUndispatched(
                                                                                             next_slot_time);
        env.nondf_wait_t_total=total_wt;
        env.nondf_wait_t_cnt = cnt;
        env.nondf_wait_t_max = max_wt;
        env.num_nondf_dispatched = num_dispatched;

        env.num_df_dispatched = wait_task_q.num_task_dispatched;

#ifdef STATE_INFO
        spdlog::info("Nondf wait time, mean {}, max {}, min {}",ave,max,min);
#endif
        nondf_wait_task_q.waiting_time.clear(); // reset
        wait_task_q.num_task_dispatched=0; // reset
    }
    tuple<long,long> getClusterOnServerLoadInfo() const {
        long total_avail = 0;
        long total_cores = 0;
        for(const auto& [id,svr]:servers){
            if(svr.isOn()){
                total_cores+=svr.getTotalCores();
                total_avail+=svr.getAvailCores();
            }
        }
        return {total_cores,total_avail};
    }

};

////////////////////////////////////// For Batch Arrival ////////////////////////////////////////////////////////////
////////////////////////////////////// For Batch Arrival ////////////////////////////////////////////////////////////
////////////////////////////////////// For Batch Arrival ////////////////////////////////////////////////////////////

class ClusterBatch {
public:
    explicit ClusterBatch(int num_server,
                     int slot_dur, // in second
                     long curr_slot_time,
                     int num_block_task_allow,
                     double temp_in_max,
                     double temp_in_min,
                     double temp_pen_coeff,
                     double backlog_pen_coeff,
                     double backog_nondf_pen_coeff,
                     double green_usage_reward_coeff,
                     double pue_cap_max,
                     double pue_cap_min,
                     int max_dispatch_failure_allowed,
                     std::string reward_f,
                     std::string log_level,
                     std::string logger_name,
                     std::string ac_cooling_model,
                     std::vector<std::tuple<int, std::string, int>> server_specs // <name,type,total cores>

    ) : num_server(num_server),
        slot_duration(slot_dur),
        num_block_allowed(num_block_task_allow),
        max_dispatch_failure_allowed(max_dispatch_failure_allowed),
        curr_slot_time(curr_slot_time),
        next_slot_time(slot_dur + curr_slot_time),
        ac_cool_model(ac_cooling_model),
        all_sleep_server_lst(),
        wait_task_q(),
        nondf_wait_task_q(),
        run_q(comp_running_q),
        env{25,0,0,
            0,0,0,0,0
                ,0,0,0},
        reward_obj(temp_in_max, temp_in_min, temp_pen_coeff,
                   backlog_pen_coeff,backog_nondf_pen_coeff,green_usage_reward_coeff,
                   pue_cap_max, pue_cap_min,reward_f),
        NUM_CORES_WKUP(0),
        AVE_WAKEUP_DURATION(15){
        spdlog::shutdown(); //clear logger
        if (logger_name=="console"){
            this->logger = spdlog::stdout_color_mt("console");
        }else{
            // NOTE,default is append
            this->logger = spdlog::basic_logger_mt("file_logger", "./runtime_log/"+logger_name);
            logger->set_pattern("%v");
        }
        if (log_level=="warn"){
            logger->set_level(spdlog::level::warn);
        }

        long total_avail_re = 0;
        long cores;
        for (auto &tp:server_specs) {
            long name(std::get<0>(tp));
            cores = std::get<2>(tp);
            Server svr(name, std::get<1>(tp), cores, 1);
            servers.insert(std::make_pair(name, svr));
            all_alive_server_lst.push_back(name);
            total_avail_re += cores;
        }
        //print.print("numServer:",num_server,"pueCapMax:",pue_cap_max);
        total_avail_res = total_avail_re;
        //print.print("Servers:",servers);
        assert( (servers.size()==num_server) );
    };


    void logAllServerState(){
        std::ostringstream oss;
        oss<<"serverState#";
        for(long i=0;i<num_server;++i){
            oss<<i<<" "<<servers.at(i).getState()<<"#";
        }
//        for(auto& p:servers){
//            oss<<p.first<<" "<<p.second.getState()<<"#";
//        }
        logger->info(oss.str());
    }
    std::tuple<vector<long>, vector<pair<long, long>>> selectServersToWkupSleepAndUpdate(long alloc,
                                                                                         vector<long> &svr_cost_energy) {
        //<wkupVec,<sleepSvrID,timeTaskFinish>>
        //NOTe, this method updates server states
        //                   front         front
        //  ----------[xd,xd ]--------------[xd xd xe xe]-------
        //              \----sleeplist         \-----aliveList
        // Note: the selection of server to sleep is different than real-implementation (no sorting
        // based on # of task left)

        svr_cost_energy.insert(svr_cost_energy.begin(), all_alive_server_lst.begin(),
                               all_alive_server_lst.end());
        long avail_resource = getCurrTotalAvailRes(); //total cores of on servers
        if (alloc > avail_resource) { // wake up
            vector<long> res;
            long diff = alloc - avail_resource;
            while (diff > 0 && !all_sleep_server_lst.empty()) {
                auto svr = all_sleep_server_lst.front();
                res.push_back(svr);
                all_alive_server_lst.push_front(svr);
                all_sleep_server_lst.pop_front();
                servers.at(svr).setOn();
                svr_cost_energy.push_back(svr); // new alive servers that needs get considered for energy cost
                diff -= servers.at(svr).getTotalCores();
            }
            return {res, {}};
        } else if (alloc < avail_resource) { // sleep servers
            vector<pair<long, long>> res_tuple_vec; //to sleep server and the last task finish time
            long diff = avail_resource - alloc;
            while (diff >=servers.at(all_alive_server_lst.front()).getTotalCores() //need enough redundancy to suspend a server
                   && all_alive_server_lst.size() > 1) { //always keep one server alive
                long svr = all_alive_server_lst.front(); //svr to be sleep
                // since there is no task will be allocated in the server, the last finished task time could be resolved
                res_tuple_vec.emplace_back(svr, servers.at(svr).getLastFinishedTaskTime(id_tsk_mp, curr_slot_time));
                all_sleep_server_lst.push_front(svr);
                all_alive_server_lst.pop_front();
                servers.at(svr).setUnschedulable(); // on but not ready
                diff -= servers.at(svr).getTotalCores();
            }
            return {{}, res_tuple_vec};
        }
        return {{},
                {}};
    }
    std::tuple<vector<long>, vector<pair<long, long>>> selectServersToWkupSleepAndUpdate_2(long alloc,
                                                                                           vector<long> &svr_cost_energy) {
        //<wkupVec,<sleepSvrID,timeTaskFinish>>
        //NOTe, this method updates server states
        //                   front         front
        //  ----------[xd,xd ]--------------[xd xd xe xe]-------
        //              \----sleeplist         \-----aliveList
        // Note: the selection of server to sleep is different than real-implementation (no sorting
        // based on # of task left)

        svr_cost_energy.insert(svr_cost_energy.begin(), all_alive_server_lst.begin(),
                               all_alive_server_lst.end());
        long avail_resource = getCurrTotalAvailRes(); //total cores of on servers
        if (alloc > avail_resource) { // wake up
            vector<long> res;
            long diff = alloc - avail_resource;
            while (diff > 0 && !all_sleep_server_lst.empty()) {
                auto svr = all_sleep_server_lst.front();
                res.push_back(svr);
                all_alive_server_lst.push_front(svr);
                all_sleep_server_lst.pop_front();
                servers.at(svr).markWakingUp(NUM_CORES_WKUP);
                svr_cost_energy.push_back(svr); // new alive servers that needs get considered for energy cost
                diff -= servers.at(svr).getTotalCores();
            }
            return {res, {}};
        } else if (alloc < avail_resource) { // sleep servers
            vector<pair<long, long>> res_tuple_vec; //to sleep server and the last task finish time
            long diff = avail_resource - alloc;
            while (diff >=servers.at(all_alive_server_lst.front()).getTotalCores() //need enough redundancy to suspend a server
                   && all_alive_server_lst.size() > 1) { //always keep one server alive
                long svr = all_alive_server_lst.front(); //svr to be sleep
                // since there is no task will be allocated in the server, the last finished task time could be resolved
                res_tuple_vec.emplace_back(svr, servers.at(svr).getLastFinishedTaskTime(id_tsk_mp, curr_slot_time));
                all_sleep_server_lst.push_front(svr);
                all_alive_server_lst.pop_front();
                servers.at(svr).setUnschedulable(); // on but not ready
                diff -= servers.at(svr).getTotalCores();
            }
            return {{}, res_tuple_vec};
        }
        return {{},
                {}};
    }

    void enqueue(long id, int duration, int req, long start_ddl, long arr_t, string type) {
        // id,duration,resource_req,start_ddl,arr_t
        // enqueue wait q and ownship q
        const auto[it, success] = id_tsk_mp.insert(
                make_pair(id, std::make_unique<Task>(id, type, duration, req, arr_t, start_ddl)));
        if (success) {
            if (type=="df"){
                wait_task_q.enqueueDefer(it->second.get());
#ifdef ENQUEUE_INFO
                spdlog::info("Defer Task enqueue succeeds with id: {}", id);
#endif
            }else{
                nondf_wait_task_q.enqueueNonDf(it->second.get());
#ifdef ENQUEUE_INFO
                spdlog::info("NonDefer Task enqueue succeeds with id: {}", id);
#endif
            }
        } else {
            spdlog::error("Task enqueue fails with id: {}", id);
        }

    }

// ############################################### Core Output Function ####################################
    std::tuple<long, long,long, long, long,long, double,double,long,long,long,long,long,double,long,long> getClusterState(){
        wait_task_q.updateDeferTaskQStatistics(next_slot_time, slot_duration); // the "next_slot_time" is the time of the coming slot
        env.num_df_dispatched = wait_task_q.num_task_dispatched;
        wait_task_q.num_task_dispatched=0; // reset
        auto [total_cores,total_avaliable_cores] = getClusterOnServerLoadInfo();

        return make_tuple(wait_task_q.num_task_expired,
                          wait_task_q.num_task_to_expired,
                          wait_task_q.num_demand_expired,
                          wait_task_q.num_demand_to_expired,
                          wait_task_q.getQueueSize(),
                          nondf_wait_task_q.getQueueSize(),
                          env.next_temp,
                          env.cluster_utilization, //cluster-wide
                          wait_task_q.nearest_ddl_in_slot,
                          env.num_df_dispatched,
                          env.num_nondf_dispatched,
                          total_cores,
                          env.OldestNonDfTaskArrivalTimeDistance,
                          env.nondf_wait_t_total,  // total waiting (dispatched + in queue)
                          total_avaliable_cores,
                          nondf_wait_task_q.getTotalResourceReq()
        );
    }
    std::tuple<long, long,long, long, long,long, double,double,long,long,long,long,long,double,long,long> getClusterStateInit(){
        // merely for initialize
        wait_task_q.updateDeferTaskQStatistics(0, slot_duration); // NOTE, difference
        env.num_df_dispatched = wait_task_q.num_task_dispatched;
        wait_task_q.num_task_dispatched=0; // reset
        auto [total_cores,total_avaliable_cores] = getClusterOnServerLoadInfo();

        return make_tuple(wait_task_q.num_task_expired,
                          wait_task_q.num_task_to_expired,
                          wait_task_q.num_demand_expired,
                          wait_task_q.num_demand_to_expired,
                          wait_task_q.getQueueSize(),
                          nondf_wait_task_q.getQueueSize(),
                          env.next_temp,
                          env.cluster_utilization, //cluster-wide
                          wait_task_q.nearest_ddl_in_slot,
                          env.num_df_dispatched,
                          env.num_nondf_dispatched,
                          total_cores,
                          env.OldestNonDfTaskArrivalTimeDistance,
                          env.nondf_wait_t_total,  // total waiting (dispatched + in queue)
                          total_avaliable_cores,
                          nondf_wait_task_q.getTotalResourceReq()
        );
    }
// ############################################### Core function ####################################
    /* std::unordered_map<string, double> step(int alloc, bool ac, int fan,
                                             double green_energy,
                                             double in_temp,
                                             double out_temp,
                                             int prev_fan,
                                             double prev_in_temp,
                                             double prev_out_temp,
                                             long curr_slot_t) {
         logger->info("slot {}",curr_slot_t);
         std::ostringstream oss;
         this->curr_slot_time = curr_slot_t;
         this->next_slot_time = curr_slot_t + slot_duration;
         // all action should have been capped prior to the invocation
         double cooling_energy = Power::getCoolingEnergyReal(fan, ac, slot_duration);
         // servers which will be consuming energy, will be filled up in the following method
         vector<long> svrs_cost_energy;
         auto[wake_vec, sleep_tuple_vec] = selectServersToWkupSleepAndUpdate(alloc, svrs_cost_energy);
         logAllServerState();
         for (auto svr:svrs_cost_energy){
             oss<<svr<<" ";
         }
         logger->info(oss.str());
 #ifdef DEBUG_ALG
         spdlog::warn("%%%%%%%%%%%-------------------SLOT_Time:{}--------------------------%%%%%%%%%%%%%%%",curr_slot_time);
         print.compact(true);
         print.print("wake_vec:",wake_vec,"sleepTupleVec:",sleep_tuple_vec);
         print.print("svrCostEnergy:",svrs_cost_energy);
         print.compact(false);
         printAliveSleepServers();
         printQueue();
 #endif
         resetServerUtilStatistics(curr_slot_time);
 #ifdef DEBUG_ALG
         print.print("--------Before Dispatch:---------");
         printServers();
 #endif
         dispatch(num_block_allowed,max_dispatch_failure_allowed,curr_slot_time);
 #ifdef DEBUG_ALG
         print.print("---------After Dispatch:----------");
         printServers();
         printQueue();
         printRunQ();
 #endif
         long total_suspend_time = 0;
         if (!sleep_tuple_vec.empty()) {
             total_suspend_time = rollbackServerState(sleep_tuple_vec);
         }
         logAllServerState();
 #ifdef DEBUG_ALG
         print.print("---------After Rollback:----------");
         printServers();
         printAliveSleepServers();
 #endif
         auto server_utilization_vec = getEachAverageServerUtil(svrs_cost_energy, next_slot_time);
         oss.str(std::string());
         oss.clear();
         for(auto utili:server_utilization_vec){
             oss<<utili<<" ";
         }
         logger->info(oss.str());
 #ifdef DEBUG_ALG
         print.print("ServerUtilization:",server_utilization_vec);
 #endif
         double totalServerEnergy = Power::getTotalServerEnergyCost(server_utilization_vec, slot_duration,
                                                                     total_suspend_time);
         // cluster-wide server utilization
         double clusterUtilization = computeClusterUtilization(svrs_cost_energy, server_utilization_vec);
         double next_temp = Cooling::deriveNextTempRealWithAc(fan, ac, out_temp, in_temp, clusterUtilization,
                                                                 prev_fan,
                                                              prev_in_temp, prev_out_temp,ac_cool_model);
         env.next_temp = next_temp;
         env.cluster_utilization = clusterUtilization;
         //wait_task_q.updateDeferTaskQStatistics(next_slot_time);
 #ifdef DEBUG_ALG
         spdlog::info("After update cluster state");
         printQueue();
 #endif
         // auto [num_exp, num_to_exp, num_demand_exp, num_demand_to_exp] = wait_task_q.getCurrExpiredTaskInfo();
 //        unordered_map<string, double> reward_dict = reward_obj.reward_1_1(totalServerEnergy,
 //                                                                          green_energy, num_exp, next_temp,
 //                                                                          cooling_energy);
         updateClusterStateAndComputeStatistics(next_slot_time);
         unordered_map<string, double> reward_dict = reward_obj.reward_func_obj(totalServerEnergy,
                                                                           green_energy,
                                                                           wait_task_q.num_task_expired,
                                                                           next_temp,
                                                                           cooling_energy,
                                                                           nondf_wait_task_q.getQueueSize());
         return reward_dict;
     }*/
// ############################################### Core function Done ####################################
    std::unordered_map<string, double> step(int alloc,
                                            bool ac,
                                            int fan,
                                            double green_energy,
                                            double in_temp,
                                            double out_temp,
                                            int prev_fan,
                                            double prev_in_temp,
                                            double prev_out_temp,
                                            long curr_slot_t) {
        // NOTE,difference than previous version,
        //  --> adding wake-up cost and delay into consideration, this version is to replace the previous one,
        //  --> change reward function: using total nondefer waiting time as the factor for penalty computation
        logger->info("slot {}",curr_slot_t);
        std::ostringstream oss;
        this->curr_slot_time = curr_slot_t;
        this->next_slot_time = curr_slot_t + slot_duration;
        // all action should have been capped prior to the invocation
        double cooling_energy = Power::getCoolingEnergyReal(fan, ac, slot_duration);
        // servers which will be consuming energy, will be filled up in the following method
        vector<long> svrs_cost_energy;
        auto[wake_vec, sleep_tuple_vec] = selectServersToWkupSleepAndUpdate_2(alloc, svrs_cost_energy);
        logAllServerState();
        for (auto svr:svrs_cost_energy){
            oss<<svr<<" ";
        }
        logger->info(oss.str());
        resetServerUtilStatistics(curr_slot_time);
        dispatch_2(num_block_allowed,max_dispatch_failure_allowed,curr_slot_time,wake_vec);
        long total_suspend_time = 0;
        if (!sleep_tuple_vec.empty()) {total_suspend_time = rollbackServerState(sleep_tuple_vec);}
        logAllServerState();
        auto server_utilization_vec = getEachAverageServerUtil(svrs_cost_energy, next_slot_time);
        oss.str(std::string());
        oss.clear();
        for(auto utili:server_utilization_vec){
            oss<<utili<<" ";
        }
        logger->info(oss.str());
        double totalServerEnergy = Power::getTotalServerEnergyCost(server_utilization_vec, slot_duration,
                                                                   total_suspend_time);
        // cluster-wide server utilization during last slot
        double clusterUtilization = computeClusterUtilization(svrs_cost_energy, server_utilization_vec);
        double next_temp = Cooling::deriveNextTempRealWithAc(fan, ac, out_temp, in_temp, clusterUtilization,
                                                             prev_fan,prev_in_temp, prev_out_temp,ac_cool_model);
        env.next_temp = next_temp;
        env.cluster_utilization = clusterUtilization;

        // get some statistics for reward computation
        auto [total_nondf_wait_time,nondf_sample_cnt,max_wt,dispach_cnt] =
                nondf_wait_task_q.getToalWaitingTimeIncludeUndispatched(next_slot_time); // for nondefer
        env.nondf_wait_t_total = total_nondf_wait_time; // env DS is a bridge to transfer some information
        env.nondf_wait_t_cnt = nondf_sample_cnt;
        env.nondf_wait_t_max = max_wt;
        env.num_nondf_dispatched = dispach_cnt;
        auto [exp_cnt,to_exp_cnt, exp_demand,to_exp_demand] = wait_task_q.getNumAndDemandDeferTaskExpiredToExpired(
                                                                                    next_slot_time);
        env.OldestNonDfTaskArrivalTimeDistance = nondf_wait_task_q.getOldestTaskArrivalTimeDistance(next_slot_time);
        nondf_wait_task_q.waiting_time.clear(); // reset


        unordered_map<string, double> reward_dict = reward_obj.reward_func_obj_2(totalServerEnergy,
                                                                                 green_energy,
                                                                                 exp_cnt,
                                                                                 next_temp,
                                                                                 cooling_energy,
                                                                                 nondf_sample_cnt, //NOTE,changed
                                                                                 total_nondf_wait_time);//NOTE, added
        return reward_dict;
    }

    void printServers(){print.print("Servers:\n",servers);}
    void printQueue(){print.print(wait_task_q);print.print(nondf_wait_task_q);}
    void printAliveSleepServers(){
        print.compact(true);
        print.line_terminator("");
        print.print(all_alive_server_lst,"<-alive|sleep->",all_sleep_server_lst,"\n");
        print.line_terminator("\n");
    }
    void printRunQ(){
        print.print("[--------********* RunQ *********-------]");
        print.line_terminator("");
        for(auto t_ptr:run_q){
            print.print(*t_ptr);
        }
        print.line_terminator("\n");
    }

private:
    const int num_server;
    const int slot_duration; //duration in seconds
    const long num_block_allowed; // how many tasks can be skipped when try to dispatching defer task
    const long max_dispatch_failure_allowed;
    long curr_slot_time;
    long next_slot_time;

    long total_avail_res;
    string ac_cool_model;

    //---------------------------------
    unordered_map<long, Server> servers; // name->server
    std::deque<long> all_alive_server_lst;
    std::deque<long> all_sleep_server_lst;
    WaitTaskQueue wait_task_q;
    NoneDeferWaitTaskQueue nondf_wait_task_q;
    //RunningTaskQueue run_q;
    std::multiset<Task *, decltype(comp_running_q)> run_q;
    unordered_map<long, std::unique_ptr<Task>> id_tsk_mp; // unique pointer,for auto-destroy task object
    Env env;
    Reward reward_obj;
    std::shared_ptr<spdlog::logger> logger;
    const long NUM_CORES_WKUP;
    const long AVE_WAKEUP_DURATION;

    void resetServerUtilStatistics(long curr_time) {
        for (auto &[name, svr]: servers) {
            if (!svr.isOff()) {
                svr.resetCumuUtil(curr_time);
            }
        }
    }

    long getCurrTotalAvailRes() const {
        long avail_resource = 0;
        for (const auto&[name, server]:servers) {
            assert((!server.isUnschedulable()));
            if (server.isOn()) {
                avail_resource += server.getTotalCores();
            }
        }
        return avail_resource;
    }

    /*void dispatch(long num_block_task_allow, long max_failure_allowed,long curr_slot_time) {
        *//* (1) A task arrives at time t is ok to be dispatched at time t.
         * (2) The reason why at the beginning there is a Separate dispatch attempt is:
         *   if they were eliminated, a task that arrives at exactly "curr_slot_time" may not be dispatched immediately.
         *   (each new slot allocate or de-allocate resources can cause dispatch event)
         * (3) Always only when non-defer tasks have been dispatched, can defer tasks be dispatched.
         * *//*
        // one time dispatch:
        assert(( run_q.empty() || (*run_q.begin())->finish_t >= curr_slot_time));
        // NOTE, it does not matter this one-time dispatch does not remove finished task, b/c the eventTrigger method
        //      will remove the finished taske (finished exactly at curr_slot_time) and dispatch it if possible;
        dispatchNondeferUntilFail(servers, nondf_wait_task_q, run_q, curr_slot_time);
#ifdef DEBUG_DISPATCH
        spdlog::info("[After One time noneDefer dispatch]");
        printRunQ();
        printServers();
#endif
        if (nondf_wait_task_q.numArriveEarlyOrEqualThan(curr_slot_time) == 0) {
            dispatchDeferBackFillUntilFail(servers, run_q, wait_task_q, curr_slot_time,
                                           num_block_task_allow,max_failure_allowed,curr_slot_time);
#ifdef DEBUG_DISPATCH
            spdlog::info("[After one time Defer dispatch]");
            printRunQ();
            printServers();
#endif
        }
        eventTriggerDispatch(wait_task_q,nondf_wait_task_q,run_q,servers,id_tsk_mp,next_slot_time,
                             num_block_task_allow,max_failure_allowed,curr_slot_time,*logger);
    }*/
    void dispatch_2(long num_block_task_allow, long max_failure_allowed,long curr_slot_time,vector<long>& wkup_lst) {
        /* (1) A task arrives at time t is ok to be dispatched at time t.
         * (2) The reason why at the beginning there is a Separate dispatch attempt is:
         *      each new slot allocate or de-allocate resources can cause dispatch event
         * (3) Always only when non-defer tasks have been dispatched, can defer tasks be dispatched.
         * */
        assert(( run_q.empty() || (*run_q.begin())->finish_t >= curr_slot_time));
        eventTriggerDispatch_2(wait_task_q,nondf_wait_task_q,run_q,servers,id_tsk_mp,next_slot_time,
                               num_block_task_allow,max_failure_allowed,curr_slot_time,*logger,
                               wkup_lst,
                               AVE_WAKEUP_DURATION);
    }

    long rollbackServerState(vector<pair<long, long>> &sleep_tuple_vec) {
        //possibly roll-back svr from sleep list,
        // make sure the state of a server is either On or Off at each slot beginning
        // when the last task finished in the unschedulable server, it will go to sleep; this value will be used to
        // revise the server energy consumption. (the current server power consumption computation method
        // does not account for the suspended state)
        long total_sleep_time = 0;
        auto iter = all_sleep_server_lst.begin(); // front is the last in; sleep_tuple_vec: back is last in
        for (int i = sleep_tuple_vec.size() - 1; i >= 0; i--) { //NOTE, careful when vector.size()-xxx
            assert((*iter == sleep_tuple_vec[i].first));
            if (sleep_tuple_vec[i].second >= next_slot_time) { // running until next slot
                servers.at(*iter).setOn(); // set on
                all_alive_server_lst.push_front(*iter);
                iter = all_sleep_server_lst.erase(iter);
            } else {
                total_sleep_time+= (next_slot_time - sleep_tuple_vec[i].second); //+how long this server been suspended
                servers.at(*iter).setOff(); // set off
                ++iter;
            }
        }
        return total_sleep_time;
    }

    vector<double> getEachAverageServerUtil(const vector<long> &svr_id_vec, long until_time) {
        // get average utilization between the latest reset time and the this method's invocation time (until_time)
        vector<double> res;
        for (const auto svr_id: svr_id_vec) {
            res.push_back(servers.at(svr_id).getCumuUtilizationUntil(until_time));
        }
        return res;
    }

    double computeClusterUtilization(vector<long> &servers_up, vector<double> &ave_utilizations) const {
        double num_up_svr = servers_up.size(); // up servers during the slot
        return (num_up_svr / num_server) * (std::accumulate(
                ave_utilizations.begin(), ave_utilizations.end(), 0.0) / ave_utilizations.size());
    }
    /*long getTotalCoresCurrentOn() const{
        long total = 0;
        for(auto& [id,svr] : servers){
            if(svr.isOn()){
                total+=svr.getTotalCores();
            }
        }
        return total;
    }*/

 /*   void updateClusterStateAndComputeStatisticsBatch(long next_slot_time) {
        //env.instant_on_server_utilization = getClusterInstantUtilizationForOnServer();
        env.total_cores_current_on = getTotalCoresCurrentOn();
        wait_task_q.updateDeferTaskQStatistics(next_slot_time, slot_duration);
//        auto [ave,max,min,total_cnt] = nondf_wait_task_q.getAveWaitingTime();
//        env.nondf_wait_t_ave = ave;
//        env.nondf_wait_t_max = max;
//        env.nondf_wait_t_min = min;
//        env.num_nondf_dispatched = total_cnt;
        env.num_df_dispatched = wait_task_q.num_task_dispatched;
        nondf_wait_task_q.waiting_time.clear(); // reset
        wait_task_q.num_task_dispatched=0; // reset
    }*/

    tuple<long,long> getClusterOnServerLoadInfo() const {
        long total_avail = 0;
        long total_cores = 0;
        for(const auto& [id,svr]:servers){
            if(svr.isOn()){
                total_cores+=svr.getTotalCores();
                total_avail+=svr.getAvailCores();
            }
        }
        return {total_cores,total_avail};
    }
};


PYBIND11_MODULE(simulator, m) {
    m.doc() = "pybind11 simulator module"; // optional module docstring
    py::class_<Cluster>(m, "Cluster")
            .def(py::init<int,
                    int , // in second
                    long,
                    int ,
                    double ,
                    double ,
                    double ,
                    double ,
                    double ,
                    double ,
                    double ,
                    double ,
                    int,
                    std::string,
                    std::string,
                         std::string,
                         std::string,
                    std::vector<std::tuple<int, std::string, int>> // <name,type,total cores>
                    >(),py::arg("num_svr"),
                 py::arg("slot_dur"),
                 py::arg("curr_slot_time"),
                 py::arg("num_block_allow"),
                 py::arg("t_in_max"),
                 py::arg("t_in_min"),
                 py::arg("t_pen_coeff"),
                 py::arg("backlog_pen_coeff"),
                 py::arg("backlog_nondf_pen_coeff"),
                 py::arg("green_reward_coeff"),
                 py::arg("pue_cap_max"),
                 py::arg("pue_cap_min"),
                 py::arg("max_dispatch_failure"),
                 py::arg("reward_f"),
                    py::arg("log_level"),
                 py::arg("logger_name"),
                 py::arg("ac_cooling_model"),
                 py::arg("svr_spec")
            )
            .def("enqueue",&Cluster::enqueue,
                    py::arg("id"),
                    py::arg("duration"),
                    py::arg("req"),
                    py::arg("start_ddl"),
                    py::arg("arr_t"),
                    py::arg("type")
                    )
                    .def("getClusterState",&Cluster::getClusterState)
                    .def("printServers",&Cluster::printServers)
                    .def("printQueue",&Cluster::printQueue)
                    .def("printAllAliveSleep",&Cluster::printAliveSleepServers).
                    def("step",&Cluster::step,
                            py::arg("alloc"),
                            py::arg("ac"),
                            py::arg("fan"),
                            py::arg("green"),
                            py::arg("in_temp"),
                            py::arg("out_temp"),
                            py::arg("prev_fan"),
                            py::arg("prev_in_temp"),
                            py::arg("prev_out_temp"),
                            py::arg("curr_slot_time")
                            )
                            .def("printRunQ",&Cluster::printRunQ);

    py::class_<ClusterBatch>(m, "ClusterBatch")
            .def(py::init<int,
                         int , // in second
                         long,
                         int ,
                         double ,
                         double ,
                         double ,
                         double ,
                         double ,
                         double ,
                         double ,
                         double ,
                         int,
                         std::string,
                         std::string,
                         std::string,
                         std::string,
                         std::vector<std::tuple<int, std::string, int>> // <name,type,total cores>
                 >(),py::arg("num_svr"),
                 py::arg("slot_dur"),
                 py::arg("curr_slot_time"),
                 py::arg("num_block_allow"),
                 py::arg("t_in_max"),
                 py::arg("t_in_min"),
                 py::arg("t_pen_coeff"),
                 py::arg("backlog_pen_coeff"),
                 py::arg("backlog_nondf_pen_coeff"),
                 py::arg("green_reward_coeff"),
                 py::arg("pue_cap_max"),
                 py::arg("pue_cap_min"),
                 py::arg("max_dispatch_failure"),
                 py::arg("reward_f"),
                 py::arg("log_level"),
                 py::arg("logger_name"),
                 py::arg("ac_cooling_model"),
                 py::arg("svr_spec")
            )
            .def("enqueue",&ClusterBatch::enqueue,
                 py::arg("id"),
                 py::arg("duration"),
                 py::arg("req"),
                 py::arg("start_ddl"),
                 py::arg("arr_t"),
                 py::arg("type")
            )
            .def("getClusterState",&ClusterBatch::getClusterState)
            .def("getClusterStateInit",&ClusterBatch::getClusterStateInit)
            .def("printServers",&ClusterBatch::printServers)
            .def("printQueue",&ClusterBatch::printQueue)
            .def("printAllAliveSleep",&ClusterBatch::printAliveSleepServers).
                    def("step",&ClusterBatch::step,
                        py::arg("alloc"),
                        py::arg("ac"),
                        py::arg("fan"),
                        py::arg("green"),
                        py::arg("in_temp"),
                        py::arg("out_temp"),
                        py::arg("prev_fan"),
                        py::arg("prev_in_temp"),
                        py::arg("prev_out_temp"),
                        py::arg("curr_slot_time")
            )
            .def("printRunQ",&ClusterBatch::printRunQ);
}

int main() {
    /*auto imax = std::numeric_limits<long>::max();
    auto imax2 = std::numeric_limits<std::int64_t>::max();
    spdlog::info("max long {}", imax);
    spdlog::info("max long {}", imax2);
    spdlog::warn("Easy padding in numbers like id:{} {}", 12, "world");
    pprint::PrettyPrinter print;
    vector<long> v{1, 2, 3};
    print.print(v);
    long x = 10000;
    double d = x;
    spdlog::info("double {}", d);*/
    //std::cout<<"hello"<<std::endl;
    struct comp_test{
        bool operator () (const int& l,const int& r){
            return l>r;
        }
    };
    // auto comp_lambda = [] (const int&l, const int& r){return l>r;};
    std::set<int, comp_test> s;
    s.insert(1);
    print.print(s);
    return 0;
};
