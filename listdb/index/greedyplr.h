#ifndef LISTDB_GREEDYPLR_H_
#define LISTDB_GREEDYPLR_H_
#include <cstddef>
#include <iostream>
#include <vector>

#include "listdb/index/greedyplr_entities.h"
#include "listdb/index/greedyplr_operations.h"

namespace PLR {
    using namespace Entities;
    using namespace Operations;

    class GreedyPLR {
    public:
        GreedyPLR() = delete;
        GreedyPLR(const double &err) : error(err) {};
        GreedyPLR(const double &err, const size_t &capacity) : error(err) {
            if(capacity<sizeof(GreedyPLR)) MaxSegNum = 0;
            else MaxSegNum = (uint64_t)((capacity-sizeof(GreedyPLR))/(sizeof(Segment)));
        };
        GreedyPLR(const GreedyPLR &) = delete;
        GreedyPLR(GreedyPLR &&) = default;
        auto operator=(const GreedyPLR &) -> GreedyPLR & = delete;
        auto operator=(GreedyPLR &&) -> GreedyPLR & = default;

        static auto make_greedy_plr(const double& err) -> std::unique_ptr<GreedyPLR> {
            return std::make_unique<GreedyPLR>(err);
        }

        auto predict(const Key& key) -> uint64_t {

            uint64_t x = key.key_num();

            if (need != -1) {
                return {};
            }
            //dangle case
            if (dangle && (double)x >= tail.x) {
                return (uint64_t)tail.y;                
            }

            auto sz = segments.size();

#if 1 // with binary search
            uint64_t h = sz - 1;
            uint64_t l = 0;
            uint64_t p=(h+l)/2;

            while(h>l){
                p=(h+l)/2;

                if(segments[p].start > x){
                    if(p==0) return segments[p].predict(x);
                    h = p-1;
                    continue;
                }
                else if(segments[p].start < x){
                    if(p==sz-1) return segments[sz-1].predict(x);
                    l = p+1;
                    continue;
                }
                else return segments[p].predict(x);
            }

            if(segments[p].start <= x){
                while(p<sz-1){
                    if(segments[p + 1].start > x) return segments[p].predict(x);
                    p++;
                }
            }
            else{
                while(p>0){
                    p--;
                    if(segments[p].start <= x) return segments[p].predict(x);
                }
            }
            
            if(p==0){
                if(segments[p].start >= x) return 0;
                return segments[p].predict(x);
            }
            else if(p==sz-1) return segments[p].predict(x);

            printf("error case exist!\n");
            return 0;

#else // without binary search
            for (uint64_t i = 0; i < sz - 1; i++) {
                if (segments[i].start <= x && segments[i + 1].start > x) {
                    return segments[i].predict(x);
                }
            }
            return segments[sz - 1].predict(x);
            
#endif

            

            
        }

        // if all data points are ready, use this
        // true: trainning is sucessful
        // false: something goes wrong
        auto train(const Key* key_array, uint64_t num_points) -> bool {
            for(uint64_t i=0; i<num_points; i++){
                Point p((double)key_array[i].key_num(),i);
                if(!iterate_on(p)) return false;
            }
        
            return finish();
        }

        // if the data pointer are delivered via an iterator
        // true: trainning is sucessful
        // false: something goes wrong
        // remember to call finish() after iteratin over all the points
        auto iterate_on(const Point &p) -> bool {
            switch (need) {
            case 2: {
                s1 = p;
                need = 1;
                return true;
            }
            case 1: {
                s2 = p;
                need = 0;
                // here we follow the naming in the paper
                auto sa = error_upper(s1, error);
                auto sb = error_lower(s1, error);
                auto sc = error_lower(s2, error);
                auto sd = error_upper(s2, error);

                plr_lower.initialize_from(sa, sc);
                plr_upper.initialize_from(sb, sd);
                s0 = interception_of(plr_lower, plr_upper);
                return true;
            }
            case 0: {
                int rv = try_consume(p);
                if (rv==1) {
                    return true;
                } else if(rv==0) {
                    need = 1;
                    s1 = p;
                    return true;
                } else {
                    //case of stop training;
                    return false;
                }
            }
            default: {
                std::cerr << "Unexpected value of need: " << need << "\n";
                return false;
            }
            }
        }

        auto dump() const noexcept {
            for (const auto & seg : segments) {
                seg.dump();
            }

            if (dangle) {
                tail.dump();
            } else {
                std::cout << "No dangling point\n";
            }
        }

        auto report() const noexcept {
            printf("MaxSegNum is %lu\n",MaxSegNum);//test juwon
            std::cout << segments.size() << " segments are trained\n";
            if (dangle) {
                std::cout << "Tail is usable: ";
                tail.dump();
            }
        }
        
    private:
        auto try_consume(const Point &p) -> int {
            auto should_yield = above_line(p, plr_upper) || below_line(p, plr_lower);
            if (should_yield) {
                // this point is out of the bound
                // start a new segment and return
                if(yield_current_segment()==-1) return -1;
                return 0;
            }

            auto p_upper = error_upper(p, error);
            auto p_lower = error_lower(p, error);

            if (below_line(p_upper, plr_upper)) {
                plr_upper.update_to(s0, p_upper);
            }

            if (above_line(p_lower, plr_lower)) {
                plr_lower.update_to(s0, p_lower);
            }

            return 1;
        }

        auto yield_current_segment() -> int {
            switch (need) {
            case 2: {
                // no segment now, just leave
                return 0;
            }
            case 1: {
                // a dangling point
                tail = s1;
                dangle = 1;
                return 1;
            }
            case 0: {
                current_segment.line.initialize_from(get_average_slope(plr_lower, plr_upper), s0);
                current_segment.start = s1.x;
                //if capacity is full, stop training
                if(segments.size()+1 > MaxSegNum) return -1;
                segments.push_back(current_segment);
                return 1;
            }
            default: {
                std::cerr << "Unexpected value of need: " << need << "\n";
                return 0;
            }
            }
        }

        // stop right now, yielding the traning segment unconditionally
        auto finish() -> bool {
            if(yield_current_segment()==-1) return false;
            need = -1;
            return true;
        }

    private:
        double error;
        std::vector<Segment> segments;
        Segment current_segment;
        Point s1, s2, s0;
        Line plr_lower, plr_upper;

        // if the sequece has a dangling point, store it here
        Point tail;
        bool dangle = false;
        int need = 2;

        //maximum number of segment for given capacity
        uint64_t MaxSegNum;
    };
}
#endif //LISTDB_GREEDYPLR_H_
