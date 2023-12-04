#ifndef __PLR__OPERATIONS__OPERATIONS__
#define __PLR__OPERATIONS__OPERATIONS__
#include "listdb/index/greedyplr_entities.h"

namespace PLR::Operations {
    using namespace Entities;

    auto slope(const Point &p1, const Point &p2) -> double {
        return (p1.y - p2.y) / (p1.x - p2.x);
    }

    auto interception_of(const Line &l1, const Line &l2) -> Point {
        //N x;
        //if(l1.slope == l2.slope) x = 0; // problem occurs here juwon
        auto x = (l2.intercept - l1.intercept) / (l1.slope - l2.slope);
        auto y = l1.get_y(x);
        return Point(x, y);
    }

    auto above_line(const Point &p, const Line &line) -> bool {
        auto predicted = line.get_y(p.x);
        return p.y > predicted;
    }

    auto below_line(const Point &p, const Line &line) -> bool {
        auto predicted = line.get_y(p.x);
        return p.y < predicted;
    }

    auto error_upper(const Point &p, const double &error) -> Point {
        return Point(p.x, p.y + error);
    }

    auto error_lower(const Point &p, const double &error) -> Point {
        return Point(p.x, p.y - error);
    }

    auto get_average_slope(const Line &l1, const Line &l2) -> double {
        return (l1.slope + l2.slope) / 2.0;
    }
}
#endif
