#ifndef __PLR__ENTITIES__ENTITIES__
#define __PLR__ENTITIES__ENTITIES__

#include <memory>
#include <iostream>

namespace PLR::Entities {

    struct Point {
        double x = 0;
        double y = 0;

        explicit Point(double x_, double y_) : x(x_), y(y_) {};
        Point() = default;
        Point(const Point &) = default;
        Point(Point &&) = default;
        auto operator=(const Point &) -> Point & = default;
        auto operator=(Point &&) -> Point & = default;

        static auto make_point(double x, double y) -> std::unique_ptr<Point> {
            return std::make_unique<Point>(x, y);
        }

        auto dump() const noexcept -> void {
            std::cout << "Point: (" << x << ", " << y << ")\n";
        }
    };

    struct Line {
        double slope = 0;
        double intercept = 0;

        explicit Line(double s, double i) : slope(s), intercept(i) {};
        Line() = default;
        Line(const Line &) = default;
        Line(Line &&) = default;
        auto operator=(const Line &) -> Line& = default;
        auto operator=(Line &&) -> Line& = default;

        static auto make_line(const double &s, const double &i) -> std::unique_ptr<Line> {
            return std::make_unique<Line>(s, i);
        }

        static auto make_line(const Point &p1, const Point &p2)
            -> std::unique_ptr<Line>
        {
            if (p1.x == p2.x)
                return nullptr;
            
            auto slope = (p1.y - p2.y) / (p1.x - p2.x);
            auto intercept = p1.y - slope * p1.x;
            return std::make_unique<Line>(slope, intercept);
        }

        auto initialize_from(const Point &p1, const Point &p2) -> bool {
            if (p1.x == p2.x)
                return false;
            slope = (p1.y - p2.y) / (p1.x - p2.x);
            //printf("slope is %f\n",slope);//test uwon
            intercept = p1.y - slope * p1.x;
            return false;
        }

        auto initialize_from(const double& s, const Point &p) -> void {
            intercept = p.y - s * p.x;
            slope = s;
        }

        auto update_to(const double& s, const double& i) -> void {
            slope = s;
            intercept = i;
        }

        auto update_to(const Point &p1, const Point &p2) -> bool {
            return initialize_from(p1, p2);
        }

        auto get_y(const double& x) const noexcept -> double {
            return slope * x + intercept;
        }

        auto dump() const noexcept -> void {
            std::cout << "Line: [" << slope << ", " << intercept << "]\n";
        }
    };

    struct Segment {
        Line line;
        uint64_t start = 0;

        explicit Segment(const Line &l, double s) : line(l), start(s) {};
        Segment() = default;
        Segment(const Segment &) = default;
        Segment(Segment &&) = default;
        auto operator=(const Segment &) -> Segment& = default;
        auto operator=(Segment &&) -> Segment& = default;

        // make a new segment with slope l starting at point s
        static auto make_segment(const Line &l, double s) -> std::unique_ptr<Segment> {
            return std::make_unique<Segment>(l, s);
        }

        // make a new segment passing two points, with the first point's x being the start
        static auto make_segment(const Point &p1, const Point &p2)
            -> std::unique_ptr<Segment>
        {
            auto seg = std::make_unique<Segment>();
            seg->line.initialize_from(p1, p2);
            seg->start = p1.x;
            return seg;
        }

        auto initialize_from(const double& slope, const Point& s0) -> void {
            line.initialize_from(slope, s0);
        }

        auto predict(const double &x) -> uint64_t {
            return (uint64_t)line.get_y((double)x);
        }

        auto dump() const noexcept -> void {
            std::cout << "Segment: {" << line.slope << ", " << line.intercept
                      << ", " << start << "}\n";
        }
    };
}
#endif
