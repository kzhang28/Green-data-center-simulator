//
// Created by Kuo Zhang on 6/17/20.
//

#ifndef CPP_REWARD_H
#define CPP_REWARD_H

#include <unordered_map>
#include <string>
#include <utility>
#include <functional>
using std::string;
using std::unordered_map;

class Reward{

private:
    const double temp_in_max_;
    const double temp_in_min_;
    const double temp_pen_coeff_;
    const double backlog_pen_coeff_;
    const double backlog_nondf_pen_coeff_;

    const double green_usage_reward_coeff_;
    const double pue_cap_max_;
    const double pue_cap_min_;



    double computeTempPenalty(double temp_next) const {
        if (temp_next > temp_in_max_){
            return temp_pen_coeff_ * (temp_next - temp_in_max_);
        }else if(temp_next < temp_in_min_){
            return temp_pen_coeff_ * (temp_in_min_ - temp_next);
        }else{
            return 0.0;
        }

    }
    inline double computeBacklogPenalty (int num_task_expired) const {
        return num_task_expired*backlog_pen_coeff_;
    }
    inline double computeNonDfBacklogPenalty (int num_task_delayed) const {
            if (num_task_delayed <2) { return 0;}
            if (num_task_delayed<4){ return num_task_delayed*backlog_nondf_pen_coeff_;}
            return 2*num_task_delayed*backlog_nondf_pen_coeff_;
    }
    inline double computeNonDfBacklogPenalty_july24 (int num_task_delayed) const {
        return 2*num_task_delayed*backlog_nondf_pen_coeff_;
    }
    inline double computeNonDfBacklogPenalty_aug30 (int cnt, double total_wait_time) const {
        // difference, using the *TOTAL* nondf waiting time as the penalty parameter,instead of waiting count
        return backlog_nondf_pen_coeff_* total_wait_time;
    }

public:
    std::function<std::unordered_map<string,double>(double ,double, int,double ,double,int)> reward_func_obj;
    std::function<std::unordered_map<string,double>(double ,double, int,double ,double,int,double)> reward_func_obj_2;

    Reward(double temp_in_max,double temp_in_min,double temp_pen_coeff,
            double backlog_pen_coeff,
            double backlog_nondf_pen_coeff,
            double green_usage_reward_coeff,
            double pue_cap_max,
            double pue_cap_min,
            const string& type):temp_in_max_(temp_in_max),temp_in_min_(temp_in_min),
                                temp_pen_coeff_(temp_pen_coeff),
                                backlog_pen_coeff_(backlog_pen_coeff),
                                backlog_nondf_pen_coeff_(backlog_nondf_pen_coeff),
                                green_usage_reward_coeff_(green_usage_reward_coeff),
                                pue_cap_max_(pue_cap_max),
                                pue_cap_min_(pue_cap_min){
        // using namespace std::placeholders;
        //reward_func_obj = std::bind(&Reward::reward_1_1,this,_1,_2,_3,_4,_5); //another way
        //if (!(type.empty() || type=="_reward_1_1_new")){std::exit(1);}
        reward_func_obj = [this,type](double svr_energy,double green_energy, int num_task_expired,
                                 double next_temp, double cooling_energy,int num_nondf_delayed){
            if (type.empty()){ //default
                return this->reward_1_1(svr_energy,green_energy,num_task_expired,next_temp, cooling_energy);
            }else if(type=="_reward_1_1_new"){
                return this->reward_1_1_new(svr_energy,green_energy,num_task_expired,next_temp,cooling_energy,
                        num_nondf_delayed);
            } else if(type=="reward_1_1_new_july24"){
                return this->reward_1_1_new_july24(svr_energy,green_energy,num_task_expired,next_temp,cooling_energy,
                        num_nondf_delayed);
            } else if(type=="reward_1_1_new_aug1"){
                return this->reward_1_1_new_aug1(svr_energy,green_energy,num_task_expired,next_temp,cooling_energy,
                        num_nondf_delayed);
            }
            else{
                std::cout<<"Wrong Reward Func"<<std::endl;
                exit(1);
            }

        };
        reward_func_obj_2 = [this,type](double svr_energy,double green_energy, int num_task_expired,
                double next_temp, double cooling_energy,int cnt,double total_wait_time){
            if (type=="reward_1_1_new_aug30") {
                return reward_1_1_new_aug30(svr_energy,green_energy,num_task_expired,next_temp,cooling_energy,
                                            cnt,total_wait_time);
            }else if (type=="reward_1_1_new_aug30_v1"){
                return reward_1_1_new_aug30_v1(svr_energy,green_energy,num_task_expired,next_temp,cooling_energy,
                                            cnt,total_wait_time);
            }else {
                std::cout<<"Wrong Reward Func"<<std::endl;
                exit(1);
            }

        };
    };

    std::unordered_map<string,double> reward_1_1(double svr_energy,double green_energy, int num_task_expired,
                                                 double next_temp, double cooling_energy) {
        // this reward method is different than previous Python version simulator: (1) no distinguish between
        // defer and non-defer energy
        auto t_penalty = computeTempPenalty(next_temp);
        auto backlog_pen = computeBacklogPenalty(num_task_expired);
        double ratio = svr_energy / (cooling_energy+1e-6);
        // cap ratio
        if (ratio>pue_cap_max_){
            ratio = pue_cap_max_;
        }else if (ratio < pue_cap_min_){
            ratio = pue_cap_min_;
        }
        auto total_energy_consume = svr_energy + cooling_energy;
        double cost;
        double bonus;
        if (total_energy_consume > green_energy){
            cost = total_energy_consume -green_energy;
            bonus = green_energy * green_usage_reward_coeff_ * ratio;
        }else{
            cost = 0.0;
            bonus = total_energy_consume*green_usage_reward_coeff_ * ratio;
        }
        std::unordered_map<string,double> res {
                {"e_cost",cost},
                {"c_cost",cooling_energy},
                {"bonus",bonus},
                {"backlog_pen",backlog_pen},
                {"t_pen",t_penalty},
                {"e_workload",svr_energy},
                {"reward",-cost+bonus-backlog_pen-t_penalty}
        };
        return res;
    }
    std::unordered_map<string,double> reward_1_1_new(double svr_energy,double green_energy, int num_task_expired,
                                                 double next_temp, double cooling_energy, int num_nondf_delayed) {
        // this reward method is different than previous Python version simulator: (1) no distinguish between
        // defer and non-defer energy
        // this reward function consider how many nondf delayed (not dispatched in the slot at which the task
        // arrival), adding penalty
        auto t_penalty = computeTempPenalty(next_temp);
        auto backlog_pen = computeBacklogPenalty(num_task_expired);
        auto nondf_delay_pen = computeNonDfBacklogPenalty(num_nondf_delayed);
        double ratio = svr_energy / (cooling_energy+1e-6);
        // cap ratio
        if (ratio>pue_cap_max_){
            ratio = pue_cap_max_;
        }else if (ratio < pue_cap_min_){
            ratio = pue_cap_min_;
        }
        auto total_energy_consume = svr_energy + cooling_energy;
        double cost;
        double bonus;
        if (total_energy_consume > green_energy){
            cost = total_energy_consume -green_energy;
            bonus = green_energy * green_usage_reward_coeff_ * ratio;
        }else{
            cost = 0.0;
            bonus = total_energy_consume*green_usage_reward_coeff_ * ratio;
        }
        std::unordered_map<string,double> res {
                {"e_cost",cost},
                {"c_cost",cooling_energy},
                {"bonus",bonus},
                {"backlog_pen",backlog_pen},
                {"nondf_bl_pen",nondf_delay_pen},
                {"t_pen",t_penalty},
                {"e_workload",svr_energy},
                {"reward",-cost+bonus-backlog_pen-t_penalty-nondf_delay_pen}
        };
        return res;
    }
    std::unordered_map<string,double> reward_1_1_new_july24(double svr_energy,double green_energy, int num_task_expired,
                                                     double next_temp, double cooling_energy, int num_nondf_delayed) {
        // this reward method is different than previous Python version simulator: (1) no distinguish between
        // defer and non-defer energy
        // this reward function consider how many nondf delayed (not dispatched in the slot at which the task
        // arrival), adding penalty
        // difference than previous one; penalty for non-defer delay
        auto t_penalty = computeTempPenalty(next_temp);
        auto backlog_pen = computeBacklogPenalty(num_task_expired);
        auto nondf_delay_pen = computeNonDfBacklogPenalty_july24(num_nondf_delayed);
        double ratio = svr_energy / (cooling_energy+1e-6);
        // cap ratio
        if (ratio>pue_cap_max_){
            ratio = pue_cap_max_;
        }else if (ratio < pue_cap_min_){
            ratio = pue_cap_min_;
        }
        auto total_energy_consume = svr_energy + cooling_energy;
        double cost;
        double bonus;
        if (total_energy_consume > green_energy){
            cost = total_energy_consume -green_energy;
            bonus = green_energy * green_usage_reward_coeff_ * ratio;
        }else{
            cost = 0.0;
            bonus = total_energy_consume*green_usage_reward_coeff_ * ratio;
        }
        std::unordered_map<string,double> res {
                {"e_cost",cost},
                {"c_cost",cooling_energy},
                {"bonus",bonus},
                {"backlog_pen",backlog_pen},
                {"nondf_bl_pen",nondf_delay_pen},
                {"t_pen",t_penalty},
                {"e_workload",svr_energy},
                {"reward",-cost+bonus-backlog_pen-t_penalty-nondf_delay_pen}
        };
        return res;
    }
    std::unordered_map<string,double> reward_1_1_new_aug1(double svr_energy,double green_energy,
                                                          int num_task_expired,
                                                            double next_temp, double cooling_energy,
                                                            int num_nondf_delayed) {
        // difference than previous one: adding PUE coeff for cost
        auto t_penalty = computeTempPenalty(next_temp);
        auto backlog_pen = computeBacklogPenalty(num_task_expired);
        auto nondf_delay_pen = computeNonDfBacklogPenalty_july24(num_nondf_delayed);
        double ratio = svr_energy / (cooling_energy+1e-6);
        // cap ratio
        if (ratio>pue_cap_max_){
            ratio = pue_cap_max_;
        }else if (ratio < pue_cap_min_){
            ratio = pue_cap_min_;
        }
        auto total_energy_consume = svr_energy + cooling_energy;
        double cost;
        double bonus;
        double pue = total_energy_consume/svr_energy;
        if (total_energy_consume > green_energy){
            cost = (total_energy_consume -green_energy)*pue;
            bonus = green_energy * green_usage_reward_coeff_ * ratio;
        }else{
            cost = 0.0;
            bonus = total_energy_consume*green_usage_reward_coeff_ * ratio;
        }
        std::unordered_map<string,double> res {
                {"e_cost",cost},
                {"c_cost",cooling_energy},
                {"bonus",bonus},
                {"backlog_pen",backlog_pen},
                {"nondf_bl_pen",nondf_delay_pen},
                {"t_pen",t_penalty},
                {"e_workload",svr_energy},
                {"reward",-cost+bonus-backlog_pen-t_penalty-nondf_delay_pen}
        };
        return res;
    }
    std::unordered_map<string,double> reward_1_1_new_aug30(double svr_energy,double green_energy,
                                                          int num_task_expired,
                                                          double next_temp, double cooling_energy,
                                                          int cnt,
                                                          double total_wait_time) {
        // difference than previous one: modify non-defer penalty !
        auto t_penalty = computeTempPenalty(next_temp);
        auto backlog_pen = computeBacklogPenalty(num_task_expired);
        auto nondf_delay_pen = computeNonDfBacklogPenalty_aug30(cnt,total_wait_time);
        double ratio = svr_energy / (cooling_energy+1e-6);
        // cap ratio
        if (ratio>pue_cap_max_){
            ratio = pue_cap_max_;
        }else if (ratio < pue_cap_min_){
            ratio = pue_cap_min_;
        }
        auto total_energy_consume = svr_energy + cooling_energy;
        double cost;
        double bonus;
        double pue = total_energy_consume/svr_energy;
        if (total_energy_consume > green_energy){
            cost = (total_energy_consume -green_energy)*pue;
            bonus = green_energy * green_usage_reward_coeff_ * ratio;
        }else{
            cost = 0.0;
            bonus = total_energy_consume*green_usage_reward_coeff_ * ratio;
        }
        std::unordered_map<string,double> res {
                {"e_cost",cost},
                {"c_cost",cooling_energy},
                {"bonus",bonus},
                {"backlog_pen",backlog_pen},
                {"nondf_bl_pen",nondf_delay_pen},
                {"t_pen",t_penalty},
                {"e_workload",svr_energy},
                {"reward",-cost+bonus-backlog_pen-t_penalty-nondf_delay_pen}
        };
        return res;
    }
    std::unordered_map<string,double> reward_1_1_new_aug30_v1(double svr_energy,double green_energy,
                                                              int num_task_expired,
                                                            double next_temp, double cooling_energy,
                                                            int cnt,
                                                            double total_wait_time) {
        // different than previous one: no pue coeff for cost
        auto t_penalty = computeTempPenalty(next_temp);
        auto backlog_pen = computeBacklogPenalty(num_task_expired);
        auto nondf_delay_pen = computeNonDfBacklogPenalty_aug30(cnt,total_wait_time);

        double ratio = svr_energy / (cooling_energy+1e-6);
        // cap ratio
        if (ratio>pue_cap_max_){
            ratio = pue_cap_max_;
        }else if (ratio < pue_cap_min_){
            ratio = pue_cap_min_;
        }
        auto total_energy_consume = svr_energy + cooling_energy;
        double cost;
        double bonus;
        if (total_energy_consume > green_energy){
            cost = total_energy_consume -green_energy;
            bonus = green_energy * green_usage_reward_coeff_ * ratio;
        }else{
            cost = 0.0;
            bonus = total_energy_consume*green_usage_reward_coeff_ * ratio;
        }
        std::unordered_map<string,double> res {
                {"e_cost",cost},
                {"c_cost",cooling_energy},
                {"bonus",bonus},
                {"backlog_pen",backlog_pen},
                {"nondf_bl_pen",nondf_delay_pen},
                {"t_pen",t_penalty},
                {"e_workload",svr_energy},
                {"reward",-cost+bonus-backlog_pen-t_penalty-nondf_delay_pen}
        };
        return res;
    }


};




#endif //CPP_REWARD_H
