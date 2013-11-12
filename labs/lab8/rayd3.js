// Ray Di Ciaccio
// Lab 8
// Boston Taxi Data Visualization
(function() {
    /// Build time-block scale
    var hour_step = 3;
    var days_of_week = [ 'Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday' ],
        hour_intervals = build_hour_intervals(hour_step);

    var height = 375,
        width = 1200,
        controls_height = 175,
        padding = 30;

    var show_density = true;

    function build_hour_intervals(step) {
        var intervals = [];
        for (var i = 0; i < 23; i++) {
            if((i % step) === 0) {
                intervals.push(i);
            }
        }
        return intervals;
    }

    function build_timeblock_thresholds(days_of_week, hour_intervals) {
        var thresholds = [];
        for (var i = 0; i < days_of_week.length; i++) {
            for (var j = 0; j < hour_intervals.length; j++) {
                thresholds.push(compute_timeblock_value(i, hour_intervals[j]));
            }
        }
        // we don't need the first value as a threshold since it's time 00:00:00
        return thresholds.slice(1);
    }

    function build_timeblock_range(days_of_week, hour_intervals) {
        var range = [];
        for (var i = 0; i < (days_of_week.length * hour_intervals.length); i++) {
            range.push(i);
        }
        return range;
    }

    function compute_timeblock_value(day_of_week, hour) {
        return day_of_week +  (hour / 24);
    }

    var timeblock_scale = d3.scale.threshold()
        .domain(build_timeblock_thresholds(days_of_week, hour_intervals))
        .range(build_timeblock_range(days_of_week, hour_intervals));

    var timescale = function(date) {
        return timeblock_scale(compute_timeblock_value(date.getDay(), date.getHours()));
    }

    var time_fmt = d3.time.format('%Y-%m-%d %H:%M:%S');

    d3.select('svg.loading').append('text').text('Loading data...').attr('x', width / 2).attr('y', height / 2).attr('text-anchor','middle');

    // Event dispatcher for filtering data
    var filter_dispatch = d3.dispatch("filterchange");

    d3.select('#density_chk').on('change', function() {
        show_density = d3.event.target.checked;
        filter_dispatch.filterchange();
    });

    d3.select('#play_anim').on('click', function() {
        run_animation();
    });

    function run_animation() {
        var count = 0;
        for (var i = 0; i < days_of_week.length; i++) {
            for(var j = 0; j < hour_intervals.length; j++) {
                var fn = function(k,l) {
                    return function() {
                        var day_selector = ".day" + k;
                        var hour_selector = ".hour" + hour_intervals[l];
                        d3.selectAll('rect.timeblock').classed('selected', false);
                        d3.select('rect.timeblock' + day_selector + hour_selector).classed('selected', true);
                        filter_dispatch.filterchange();
                    };
                };
                window.setTimeout(fn(i,j), 500 * count);
                count++;
            }
        }
    }


    d3.json('pickups.json', function(error, rows) {
        // scale pickup timestamp data using timescale defined above (puts pickups into "timeblocks")
        var dataset = rows.map(function(item) {
            return {
                "lat": item["lat"],
                "lon": item["lon"],
                "name": item["name"],
                "pickups": d3.nest().key(function(d) { return timescale(time_fmt.parse(d)); }).rollup(function(d) { return d.length; }).map(item["pickups"]),
                "filtered_count": 0
            };
        });

        d3.select('svg.loading').remove();

        var xmax = d3.max(dataset, function(d) {
            return d.lon;
        });

        var ymax = d3.max(dataset, function(d) {
            return d.lat;
        });

        var xmin = d3.min(dataset, function(d) {
            return d.lon;
        });

        var ymin = d3.min(dataset, function(d) {
            return d.lat;
        });

        var xscale = d3.scale.linear()
                             .domain([xmin, xmax])
                             .range([width - (2 * padding), padding]);

        var yscale = d3.scale.linear()
                             .domain([ymin, ymax])
                             .range([height - padding, padding]);

        var build_colorscale = function(max, min) { return d3.scale.sqrt().domain([min,max]).range(['lightblue', 'darkred']); };


        var svg = d3.select('svg.map').attr('width', width).attr('height', height)
            .on('mouseout', function() {
                d3.selectAll('.popup').remove();
            })
        var svg_controls = build_controls(d3.select('svg.controls').attr('width', width).attr('height', controls_height));

        var polygons = d3.geom.voronoi()
            .x(function(d) { return xscale(d.lon); })
            .y(function(d) { return yscale(d.lat); })
            .clipExtent([[0, 0], [width, height]])
            (dataset);

        var areas = svg.selectAll('path.area').data(dataset);
        areas.enter()
            .append('path')
            .attr('d', function(d,i) { return make_polygon(polygons[i]); })
            .attr('class', 'area')
            .on('mousemove', function(d,i) {
                d3.selectAll('.popup').remove();
                var mouse_loc = d3.mouse(svg[0][0]);
                var popup = svg.append('text').attr('class','popup').text(d.name + ", " + d.filtered_count).attr('x', mouse_loc[0] + 5).attr('y', mouse_loc[1] - 5);
                if((popup.property('textLength').baseVal.value + Number(popup.attr('x'))) > width) {
                    popup.attr('text-anchor', 'end');
                }
            });
        areas.each(function(d,i) { d.area = d3.geom.polygon(polygons[i]).area(); });

        filter_dispatch.on('filterchange', applyNewFilter(areas));

        function applyNewFilter(selected_elements) {
            var elements = selected_elements;
            return function() {
                var selected_timeblocks = d3.selectAll('rect.timeblock.selected').data();
                console.log("Applying filter: " + selected_timeblocks);
                var max_val = 0;
                var min_val = 99999999;
                elements.each(function(d,i) {
                    var count = d3.sum(selected_timeblocks.map(function(timeblock) {
                        return d.pickups[timeblock];
                    }));
                    var value = (show_density ? count / d.area : count);
                    if(value > max_val) {
                        max_val = value;
                    }
                    if(value < min_val) {
                        min_val = value;
                    }
                    d.filtered_count = count;
                });
                var colorscale = build_colorscale(max_val, min_val);
                elements.style('fill', function(d) { return colorscale((show_density ? d.filtered_count / d.area : d.filtered_count)); });
            };
        }

        function make_polygon(polygon) {
            if(polygon) {
                return 'M' + polygon.join('L') + 'Z';
            }
            else {
                return '';
            }
        }

        // build time-block grid
        function build_controls(control_svg) {
            var block_padding = 2;
            var block_width = (width / (days_of_week.length + 1)) - block_padding;

            var dayscale = d3.scale.linear()
                .domain([-1, days_of_week.length])
                .range([0, width]);

            var hourscale = d3.scale.linear()
                .domain([0, hour_intervals.length - 1])
                .range([0, controls_height - (1.5 * padding)]);

            var day_labels = control_svg.selectAll('text.daylabel')
                    .data(days_of_week)
                    .enter()
                    .append('text')
                    .attr('class', function(d,i) { return 'daylabel day' + i; })
                    .text(function(d) { return d; })
                    .attr('text-anchor', 'middle')
                    .attr('x', function(d,i) { return dayscale(i) + (block_width / 2); })
                    .attr('y', function(d,i) { return controls_height - (padding / 4); })
                    .on('click', function(d,i) {
                        var on = this.classList.contains('selected');
                        d3.select(this).classed('selected', !on);
                        d3.selectAll('rect.day' + i).classed('selected', !on)
                        filter_dispatch.filterchange();
                    });

            var hour_labels = control_svg.selectAll('text.hourlabel')
                    .data(hour_intervals)
                    .enter()
                    .append('text')
                    .attr('class', function(d,i) { return 'hourlabel hour' + d })
                    .text(function(d) { return d + ':00 - ' + ((d + hour_step - 1) % 24) + ':59'; })
                    .attr('x', dayscale(0))
                    .attr('text-anchor', 'end')
                    .attr('y', function(d,i) { return hourscale(i); })
                    .attr("transform", "translate(-10," + (((controls_height - (1.5 * padding)) / hour_intervals.length) - 4) + ")")
                    .on('click', function(d,i) {
                        var on = this.classList.contains('selected');
                        d3.select(this).classed('selected', !on);
                        d3.selectAll('rect.hour' + d).classed('selected', !on)
                        filter_dispatch.filterchange();
                    });

            var timeblocks = build_timeblock_range(days_of_week, hour_intervals);

            var block = control_svg.selectAll('.timeblock')
                    .data(timeblocks)
                    .enter()
                    .append('rect')
                    .attr('class', function(d, i) { return 'timeblock hour' + ((d % hour_intervals.length) * hour_step) + ' day' + Math.floor(d / hour_intervals.length) })
                    .attr('x', function(d,i) { return dayscale(Math.floor(d / hour_intervals.length)); })
                    .attr('y', function(d,i) { return hourscale(d % hour_intervals.length); })
                    .attr('width', function(d,i) { return block_width; })
                    .attr('height', function(d,i) { return ((controls_height - (1.5 * padding)) / hour_intervals.length); })
                    .on('click', function(d,i) {
                        d3.select(this).classed('selected', !this.classList.contains('selected'));
                        filter_dispatch.filterchange();
                    });

            return control_svg;
        }
    });
})();
