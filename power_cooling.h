//
// Created by Kuo Zhang on 6/13/20.
//

#ifndef CPP_POWER_COOLING_H
#define CPP_POWER_COOLING_H
#include "common.h"
class Power {
public:
    static double getFreeCoolingEnergyReal(int fan, int duration) {
        /* duration: in min
         *
         * [ 1.21457674 -0.47502697  0.05234979]
         * Max free cooling power 425w
           MSE: 3.6475724479424207
           MAPE: 59.752843057955964
          Mean AC power: 2300.9601584095103
         * */
        // checked, most recently used
        double power = 1.21457674 - 0.47502697 * fan + 0.05234979 * fan * fan;
        if (power > 425) {
            power = 425;
        }
        return (power * duration) / 3600.0; //w*h
    }
    static double getAcEenergyReal(int duration) {
        // duration in second
        // checked, most recently used
        return (2300.0 * duration) / 3600; //w*h
    }
    static double getCoolingEnergyReal(int fan,bool ac, int duration){
        if (ac){
            return getAcEenergyReal(duration);
        }else{
            return getFreeCoolingEnergyReal(fan,duration);
        }
    }
    /*static double getServerEnergyRealXe3(int defer, int non_defer, int wakeup_duration = 0) {
        double energy =
                7.12612755e-03 * defer + 1.19382428e-03 * non_defer - 4.20011703e-04 * wakeup_duration + 1.63043866;
        return energy;

    }*/
    static double getServerEnergy(double utilization,int slot_duration_in_sec){
        // peak: 130w idle 20w
        // checked
        //NOTE, for only xe3
        double power = 20+110*utilization;
        return (power*slot_duration_in_sec)/3600.0;
    }
    static double getTotalServerEnergyCost(const vector<double>& util_vec, int slot_dur_sec,long total_suspended_time){
        double res=0.0;
        for(const auto& utl:util_vec){
            res+= getServerEnergy(utl,slot_dur_sec);
        }
        // NOTE,revise by considering the difference between suspended and idle
        res -= total_suspended_time*IDLE_SUSPEND_DIFFERENCE/3600.0;//watt*h
        assert(res>0);
        return res;
    }

};

class Cooling {
public:
    static double deriveNextTempRealWithAc(int fan, bool ac, double out_t, double in_t, double utilization, int fan_prev,
                                    double in_t_prev, double out_t_prev, const std::string& ac_cool_model) {
        //NOTE, AC cooling model need to be refined
        if (!ac) {
            // free cooling checked, mostly used
            return 4.03029402e-02 + 1.3659213 * in_t - 3.67072348e-01 * in_t_prev + 9.00665158e-02 * out_t
                   - 8.38264665e-02 * out_t_prev + 1.84091266e-02 * fan - 4.13505135e-03 * fan_prev -
                   1.43287225e-03 * fan * in_t
                   + 8.09774295e-04 * fan * out_t + 1.21660261e-03 * utilization;
        } else {
//            return -0.20458894 + 1.42825379 * in_t - 0.44145964 * in_t_prev + 0.0133313 * out_t +
//                   0.2241252 * utilization; // this model does not seem good
              if (ac_cool_model=="new") {
                  return -0.04480216 + 1.47108661*in_t -0.48059414*in_t_prev + 0.00767686*out_t +
                         0.11640525*utilization; // most recent model, Aug10,2020
              }else{
                  return 0.0914 + out_t * 0.0205 + in_t * 1.2616-in_t_prev * 0.2924; //this's the old parasol AC cooling model
              }
        }

    }


};

#endif //CPP_POWER_COOLING_H
