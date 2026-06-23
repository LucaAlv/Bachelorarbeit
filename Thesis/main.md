:::: titlepage
::: center
Bachelor's Thesis

------------------------------------------------------------------------

**The impact of extreme weather events**

------------------------------------------------------------------------

Department of Economics\
Ludwig-Maximilians-Universität München

**Luca Veh**

Munich, Month Day^th^, Year

![image](./sigillum.png){width="40%"}

Supervised by Prof. Dr. Fabian Waldinger
:::
::::

# Introduction {#intro}

## Motivation

The United States of America are regularly hit by severe storms and
tropical cyclones along its southern coastline and on its unincorporated
territory Puerto Rico. The NHC estimates that an average of 17.2
tropical cyclones reach the United States every decade. [@NHC2025]
According to the National Centers for Environmental Information (NCEI)
the estimated average cost of major extreme weather events in the United
States in the past five years alone amounts to close to 150 billion
dollars. [@Smith2020] Extreme weather events, notably tropical storms
and hurricanes can cause large short-run disruptions and the magnitude
and persistence of these effects may differ strongly across different
regions. Studying these impacts is relevant both for understanding
climate-related economic risk and for evaluating how vulnerable regions
recover after major storms.

## Research Question

My main research question is: How do tropical storms, affect local
nightlight intensity and how quickly do the affected areas recover? As a
first step, I focus on short-run local impacts using storm exposure and
nighttime lights as a proxy for economic activity. A possible extension
is to study whether repeated storm exposure also affects demographic
outcomes such as migration at a more aggregated spatial and temporal
scale.

# Data

To study the effect of storms on nightlights this analysis uses data
from three sources that will be briefly introduced here. More precisely
the following sections are going to outline why each dataset was
selected, which assumptions were made during (pre-)processing and
finally how all three data sources were harmonized into their final
shapes for further analysis.

## IBTrACS Data

\[What is ibtracs\] The main source for storm-wind data is the
International Best Track Archive for Climate Stewardship (IBTrACS), a
global tropical cyclone archive maintained by the U.S. National Oceanic
and Atmospheric Administration (NOAA). IBTrACS merges historical and
recent storm-track records from multiple meteorological agencies, and
warning centers into one standardized dataset, with coverage extending
from the nineteenth century to the present. Track points are generally
available at three (to six)-hourly intervals, making it a dense and high
quality data source. It should be noted that the IBTrACS is not a
collection of raw observations. Instead, it is based on so called \"best
track\" **estimates** produced by certain operational agencies after
considering the available evidence for each storm, such as land, ship
and buoy reports, radar, aircraft reconnaissance, satellite observations
and post-season reanalysis. The different agencies from whom the data is
sourced often use different practices, wind-averaging periods, observing
systems and reporting conventions. IBTrACS tries to preserve many of
these agency-specific variables and should thus be interpreted more as a
harmonized archive of best-track estimates rather than a fully
homogeneous observational product. \[source of ibtracs information\] For
this analysis IBTrACS provides a consolidated version of core storm
information such as time, storm name and type, location, maximum
sustained wind, minimum central pressure and much more \[source 14\],
that will be fed into the wind models, as outlined below. A
visualization of the data stored in IBTrACS can be found in the Appendix
(Link to graph). This analysis uses the NOAA/NCEI NetCDF release of this
data for the North Atlantic basin \[source 12\].

\[How do we get the wind grids from this\] The raw IBTrACS data itself
is just a set of point measurements. For this analysis however, storm
estimates are required for a larger geographic area. To infer the
estimated storm data from the point measurements a parametric wind model
is used. A parametric wind model is essentially a set of mathematical
equations used to simulate the storm surface winds based on a number of
key parameters, such as a storm's central pressure, maximum wind speed,
and radius of maximum winds.

\[Implementation in R\] In R these models are conveniently implemented
by the R package StormR \[cite package\] that manages everything, from
the extraction of the point measurement tracks, the generation of wind
fields, and (optionally) the calculation of storm metrics like maximum
sustained wind, a power dissipation index, and exposure duration. The
StormR package provides three models for its computations: a symmetrical
model developed by Holland (1980) \[cite Holland\], an asymmetrical
model developed by Boose et al. (2004) \[cite Boose\] and a second
symmetrical model based on Willoughby et al. (2006) \[Willoughby\].

\[Why Willoughby?\] The main difference between these models lies in the
storm measurements they are using for their calculations and the
resulting shape of the wind-fields. The shape of the wind-fields can
either be symmetric, meaning the model produces the same estimate for
all points with the same distance to the storm's center, resulting in
circle-shaped wind fields or it can be asymmetric, resulting in more
complex wind-fields.

Because of its reliability and its empirical nature, here the Willoughby
model is used. A more detailed discussion of the advantages and
disadvantages of the three models can be found in the Appendix \[link to
Appendix\].

\[What is the willoughby model?\] The Willoughby model is a symmetric
parametric wind model that was designed for tropical-cyclones, including
tropical storms, hurricanes typhoons and cyclones. The storms that occur
in the region of interest here (i.e. the carribean \[include correct
geographic definition here\]) are all of this type \[cite source for
storm types\]. The wind-profile formula was originally fitted and
calibrated using aircraft observations of hurricanes, making it
empirically a valid choice for this analysis.

\[How does the model work?\] Instead of simulating the full atmospheric
physics of a hurricane, the Willoughby model uses the measured IBTrACS
data to approximate the radial structure of a storm's wind field. A key
feature of the Willoughby model is that it separates the storm into an
inner-core region and an outer-wind region. In the inner-core region,
near the storm center, winds are relatively weak and then increase
rapidly when moving further away from the storm center and closer to the
radius of maximum wind (RMW). Beyond this radius wind speeds decline
exponentially, as the distance from the storm center increases outward
and further away from the storm's eye. The RMW plays a central role in
the computations for the Willoughby model, as it determines where the
peak wind speed occurs. A small RMW means that the strongest winds in
the storm are concentrated closer to the center, which in turn produces
a more compact wind field and vice-versa. The RMW is usually provided as
a measurement in the IBTrACS input storm dataset. Unfortunately for
certain values of the RMW the Willoughby model produces unreasonable
windspeed values. In particular it has difficulties handling a
combination of very high measured RMW values in an otherwise relatively
weak/remnant storm. In this scenario the inner-core exponent can become
negative causing the formula to explore near the storm center. This is
likely rather a bug than a systematic problem, that could be fixed by
just excluding those extreme speeds from the dataset. However, excluding
values always carries the risk of introducing bias, so for the sake of
consistency this analysis uses an empirical RMW, meaning all RMW values
are calculated separately from the other storm characteristics. This
does carry the risk of losing some unique details, but since it is not
the focus to most accurately model storms, the advantages of more
consistent data outweighs the risks here. In addition, there are some
more parameters in the Willoughby model that control various other
aspects of the wind field's behavior. For a brief mathematical
introduction of the concrete formula used, please refer to the Appendix
\[Appendix: write about this\].

\[Implementation\] For this analysis two complementary approaches to
estimate the wind fields are implemented. A centroid exposure measure
and a full-polygon exposure measure, allowing the analysis to compare
point-based and area-based definitions of the tropical cyclone wind
exposure fields.

\[Centroid Function\] The centroid-based approach uses StormR's
temporalBehavior function. For each county a representative point at the
center of the county is computed. Extracting centroids can be tricky,
especially for coastal counties, which are the main focus of this
analysis. The reason for this arises from the way the R functions from
the sf package, used to calculate the centroids, work. These functions
are fundamentally planar geometric operations, meaning they assume that
the coordinates provided to them lie one a flat surface. The (spatial)
geometric data sourced from the tigris package used for this analysis
however are angular coordinates on the globe in the WGS84/EPSG:4326
coordinate reference system (CRS). These work with longitudes and
latitudes, whose units are degrees, not meters. As an example, one
degree of longitude near the equator corresponds to about 111
kilometers, while one degree of longitude near the poles corresponds to
nearly zero kilometers. To avoid doing planar geometry on angular
coordinates the counties' coordinates were transformed from
longitude/latitude into a projected CRS measured in meters before
calculating the centroid. This projected CRS functions basically like a
flat map of the relevant part of the globe. It is created by calculating
a bounding box around the relevant area and setting up a so called local
Lambert Azimuthal Equal Area Projection (LAEA) CRS that is centered at
the center of the bounding box. After calculating the centroids all
county coordinates are transformed back to WGS84. Afterwards the
coordinates are fed into StormR's temporalBehavior function, which
returns a timeseries of windspeeds for every centroid. The sub-daily UTC
estimates are then converted into local calendar dates (more on this
below) and aggregated to a daily county panel.

Secondly, a polygon-based approach estimates exposure over the full
county area. It uses StormR's spatialBehaviour function to generate a
raster wind-field profile for each storm. Instead of just one point for
each county this estimates wind speeds for \"every\" point (or more
precisely every raster grid cell, depending on the spatial resolution
chosen) inside a county - the calculation of a centroid is thus not
required for this approach. The sub-daily wind-speed measures are again
converted from UTC into local calendar days and then aggregated to daily
intervals.

\[Discussion of centroid vs polygon based approach\] The advantage of
the centroid-based approach is that it is comparatively simple and
computationally lighter, as it only has to calculate the windspeeds for
one point per county. On the other hand, reducing a county to just one
location may introduce bias, especially for large counties - a storm may
for example significantly affect one part of a county while missing the
centroid completely or vice versa.

The polygon-based approach solves this issue by estimating wind exposure
across the whole county, thus allowing for a more balanced view. The
polygon maximum wind variable captures the strongest modeled exposure
anywhere in the county, while the polygon mean captures average exposure
across the county. This allows for a more nuanced view of a counties'
wind exposure. The only drawback of the polygon-based approach is that
all points in a county are weighted equally when calculating the mean
and median. Since the wind speeds are later used as feature variable to
model the exposure of a county to a storm, weighing each point inside
the county with the population size would significantly improve the
accuracy of the measures. As this would however require fine-scale
population estimates it is not part of this analysis.

\[Handling of local dates\] As the geographic area considered in this
analysis spans multiple timezones, handling them appropriately is
important for the correct temporal alignment with Blackmarble Nightlight
and ERA5 weather data. The StormR functions always return data in UTC
time. When aggregating to a daily panel in a first step it is thus
important to convert into local time before aggregation. As an example,
2020-08-24 02:00 UTC may count as 2020-08-23 in the America/Chicago
timezone. After matching each county with its corresponding timezone,
the data is converted to local time.

\[What are the actual results of this?\] The final outputs of both the
centroid and the polygon approach is a balanced daily county panel with
information on windspeeds during storms. \[Put in some example summary
statistic for one of the dfs\]. It is important to note that IBTrACS
itself does not really provide regular panel data over extended periods
of time. IBTrACS only contains data for time periods with actual storm
exposure. For this reason the daily panel has quite a lot of missing
data in the IBTrACS columns for most days. In order to properly work
with the data anyway all NAs are converted to zeroes. The storm
variables in the final panel should thus not be interpreted as raw wind
speed measurements, but more as an indicator of storm-wind exposure.
This also justifies setting NAs to zero, as in this case zero simply
indicates that there is no storm-wind exposure.

## Blackmarble Nighlight Data

\[Advantages of using satellite data\] Satellite data offer several
advantages for economic and econometric research, particularly when
studying shocks such as storms and the subsequent recovery process. In a
paper on applications of satellite data in economics Donaldson and
Storeygard (2016) \[source 17\] emphasize, remotely sensed data make it
possible to observe outcomes that are otherwise difficult, costly, or
unreliable to measure, while also providing repeated observations over
large areas at relatively low marginal cost. This is especially useful
in disaster contexts, where administrative data may be delayed,
incomplete, or unavailable for heavily affected regions. Satellite data
also provide high spatial resolution and broad geographic coverage,
allowing researchers to compare exposed and less-exposed locations
within fine geographic units and across borders using consistently
collected data. Nighttime lights data are particularly valuable because
luminosity can serve as a proxy for local economic activity, settlement
intensity, and electricity use, especially in places where conventional
economic statistics are scarce or not credible. For studies of storm
impacts, changes in nightlights before and after exposure can therefore
help trace the immediate disruption caused by the event and the pace at
which affected areas return toward pre-storm levels of activity.

\[Why nighttime lights as outcome?\] Power grids are among the most
exposed elements of infrastructure to natural catastrophes. Electricity
distribution is thus often disrupted during severe storms which directly
affects night light intensity. \[source for exposure of power grids\]

\[What is blackmarble data? Where does it come from?\] Thus, the main
outcome variable in this analysis is nighttime light intensity, sourced
from NASA's Black Marble nighttime lights product suite \[cite
blackmarble data\]. The famous \"Black Marble\" view began as a global
night-lights composite from the Suomi National Polar-orbiting
Partnership (Suomi NPP) satellite data, acquired in April and October
2012. In total it took 312 satellite orbits to collect 2.5 terabytes of
data that gave NASA a clear view of Earth's land surface and islands.
\[write some more story here\]

Today's Black Marble Product is derived from Visible Infrared Imaging
Radiometry suite (VIIRS) instruments, flown on three polar-orbiting
satellite platforms, namely the original Suomi NPP (launched October
2011), NOAA-20 (launched November 2017) and NOAA-21 (launched November
2022). Of particularly importance for the Black Marble product is the
VIIRS's Day/Night Band (DNB) sensor that is designed to detect very low
levels of visible and near-infrared light at night. Concretely the DNB
sensor receives radiance from a wide range of sources, including
artificial lights from cities, roads, industry, ports, ships, fires and
gas flares, but also natural signals, such as auroras, reflected
moonlight and much more. \[cite source 10\] Because of these other
light-emitting factors the raw satellite data is not yet suited for
further analysis. For this reason in addition to the VNP46A1 daily raw
data product, Black Marble also provides VNP46A2, a daily moonlight- and
atmosphere-**corrected** Nighttime Lights (NTL) product. This product
contains a total of nine datasets \[cite source 11\], of which the
\"Gap-Filled DNB BRDF-Corrected NTL\" is used as the main source of NTL
information in this analysis. Additionally the \"Mandatory Quality
Flag\" and \"Cloud Mask Quality Flag\" datasets provide a quality flag
value to each pixel and are used to remove low quality datapoints from
the final dataset. For this analysis all pixels with poor-quality,
potential cloud contamination, or other issues, as well as outliers are
removed, only pixels observed at night, with sufficient cloud-mask
quality, clear conditions, and no shadow, cirrus, or snow/ice
contamination are introduced in the data.

\[Discussion of limitations and validity filters and choices\] One of
the key advantages of using satellite data is that it provides
consistent information at a very fine temporal and geographic scale,
independent of local restrictions that many administrative data sources
have to face. This is however not to say that satellite data does not
have its own problems. Dealing with these and choosing appropriate
filters and quality indicators is vital for the validity of the data.
When working with the Black Marble nighttime lights data the following
choices were made:

\[Implementation in R\] For the implimentation the Black Marble data is
downloaded using the blackmarbler package \[cite package\], that allows
for easy access to NASA's Black Marble Night Light Data in R.

Similar to the IBTrACS data, the nightlight data is also aggregated to a
weekly-county panel dataset. Figure 1 below illustrates the effect of
Hurricane Maria on weekly nightlights in Puerto Rican Counties.

## ERA5 Data

ERA5 is the fifth-generation global atmospheric reanalysis produced by
ECMWF for the Copernicus Climate Change Service. It combines physical
weather-model output with observations from around the world to create a
globally consistent, hourly record of atmospheric, land, and
ocean-related variables, available from 1940 to the present. [@era5] For
this analysis, the

While ERA5 offers a large number of variables this analysis uses the
wind gust and precipitation information.

ERA5 data are provided on a regular latitude-longitude grid, typically
with a resolution of 0.25 by 0.25 degrees. These grid cells are regular
in geographic coordinates, although their physical size varies with
latitude. In the API request, the geographic area is specified as a
bounding box using its northern, western, southern, and eastern
coordinates. The Climate Data Store then returns the ERA5 grid values
covering this requested box. Before the next step the tigris input
polygon shapes are converted into the same coordinate reference system
as the ERA5 raster data, which is WGS84. Similar to the procedure in the
IBTrACS data this ensures that the polygon boundaries and the raster
grid refer to the same geographic coordinate space and can be overlaid
correctly. The ERA5 raster values are then extracted for each polygon
using an exact polygon-raster overlay. Instead of simply assigning a
polygon the value of the nearest grid cell, the extraction accounts for
how much of each ERA5 grid cell overlaps with the polygon. The function
calculates an area-weighted mean, using the physical cell area as
weights. This results in, each polygon receiving one representative ERA5
value per time layer, based on the raster cells that overlap with it.
Finally a GEOID x day panel is returned.

These are aggregated for each county-polygon using a weighted mean

## Aggregation

The goal of this section is to give an overview of past aggregation and
to explain the final aggregation steps.

\[Aggregation to seven-day periods\] So far all three datasets have been
separately aggregated into a panel with one row for each county and date
combination. Unfortunately, in spite of all the advantages of working
with satellite data, one key disadvantage is that it relies on (mostly)
undisturbed visual contact to earth's surface. During major storms, this
contact tends to be blocked because of dense cloud formation. This in
turn affects the reliability and validity of the nightlight data during
the immediate storm impact. While this does not significantly affect the
study of recovery and post storm effects it makes it hard to interpret
the data on a (finer) daily temporal scale. For this reason in a final
temporal aggregation step all data is aggregated to a seven-day period
panel. Importantly I don't rely on calendar weeks here, since they tend
to be messy and inconsistent when working across a longer time frame,
since there are some calendar weeks who consist of less than seven days.
These weeks could potentially skew the dataset or would have to be
dropped. Instead I divide the whole time period into clean seven-day
periods and drop the last period if it has less than seven days. For
three years, this results in 156 full seven-day periods.

\[Summary and overview of aggregation\] At this point the raw data that
was downloaded from the API has been transformed and aggregated quite
significantly. For the sake of transparency, here is a short summary of
the aggregation process.

IBTrACS: Centroid:

Polygons: 1. Temporal aggregation to max-wind-speed daily intervals. For
a given day each point in the raster represents the maximum wind speed
during that day. 2.1 tc-poly-max-wind: Spatial aggregation to county-max
max-wind-speed daily intervals. For a given day and polygon this step
takes all raster points that fall into the polygon and returns the
maximum value. 2.2 tc-poly-mean-wind: Spatial aggregation to county-mean
max-wind-speed daily intervals. 3. On the rare occasion, when multiple
storms affect the same date and polygon their rasters will overlap and
we get multiple wind speed values. Wind speeds are not additive and
stronger winds usually end up dominating weaker winds \[cite source
13\], so in this (very rare) scenario I take the maximum value across
storms.

\[Why not use mean values?\] Permanent structures are usually not as
affected by continuous wind exposure as they are by sudden extreme
winds. During storms it is usually single strong gusts that are
responsible for the most damage \[include source here\]. This is the
reason why most aggregation focus on taking the maximum value. Mean
values are also calculated, but they serve only as a robustness check
not as the main variables.

## Final Panel

### Stacked Event Study Panel

Running the stacked event panel requires some final processing to get
the data into the correct format. The starting point is the daily
(polygon) IBTrACS-based county panel. As a reminder, for each county and
each day, this dataset records whether a storm affected the county, the
maximum estimated wind speed over the county polygon, and the associated
storm name. Since the stacked event panel is based on the structure of
the ibtracs data, it has to be prepared separately in advance. The other
data are joined later.

--- start of ibtracs event preparation

BUILD A STORM-PERIOD EVENT PANEL Similarly to the TWFE panel, the daily
data are first mapped into the seven-day period calendar. At this point
the only difference to the TWFE panel is that this panel allows for
county-days that are exposed to multiple storms (i.e. a county
experiences multiple storms on the same day) tot have separate rows for
each storm. This means there can be multiple rows for each county-day.
For each county-period-storm combination, I calculate the maximum storm
wind speed observed during the period and the number of storm days in
that period. This creates a dataset with one row per affected county,
storm, and period. Note the difference to the first panel (for the base
models): Unlike the main county-period panel, this event-specific
exposure panel can contain more than one row for the same county-period
if multiple storms affected the same county during the same seven-day
interval. ($make_storm_period_event_panel function$)

All county-days where no storm was present are then dropped, since these
observations do not define storm events. The event-study construction
starts from this daily polygon-based IBTrACS county panel. First, daily
observations are mapped into the seven-day period calendar. County-days
without any storm name are dropped for the event-specific storm panel,
since they do not define storm exposure events. The remaining
observations are aggregated to the county-period-storm level. For each
county, period, and storm, I compute the maximum modeled storm wind
speed observed during the period and the number of storm days in that
period. This produces a panel with one row per affected county, storm,
and period. Unlike the main county-period panel, this event-specific
panel can contain multiple rows for the same county-period if more than
one storm affected the county during the same seven-day interval.

DEFINE STORM-LEVEL EVENT TIMING

Using this county-period-storm panel, I define storm-level event timing.
For each storm, the event period is defined as the first period in which
the storm appears anywhere in the sample. This timing is storm-specific,
not county-specific: it captures when a storm first enters the study
area, not necessarily when each affected county is first exposed. If a
storm first hits Florida in period 50 and Louisiana in period 51, the
storm-level event period is still period 50, since this is the period
where the storm \"enters the dataset\". (storm_event_df)

DEFINE COUNTY-SPECIFIC EXPOSURE

I also construct a county-specific exposure data set. For each affected
county-storm pair, I record the first period in which that county is
affected by that storm. This local event period may differ from the
storm-level event period when a storm reaches counties in different
periods. For example, if a storm first appears in the sample in period
90 but reaches a particular county in period 91, the storm-level event
period is 90, while that county's local event period is 91. In more
detail this dataset is also constructed from the county-period-storm
panel. By grouping the data is summarized to a GEOID x storm panel. The
same data set also records the maximum storm wind speed experienced by
the county across all exposed periods, the total number of exposed storm
days, the number of exposed periods, and whether any exposure occurred
in a county-period with multiple storms. In summary, this data set
identifies treated county-storm pairs: a county appears for a storm only
if it was actually exposed to that storm.

Two event panels are constructed: the first panel, consisting only of
exposed counties for each storm, can be used for exploratory and
descriptive analysis, the second panel, consisting of both exposed and
control counties, will be used for the actual causal identification
strategy.

CREATE TREATED-ONLY EVENT PANEL

The treated-only event panel contains only county-storm pairs that were
actually exposed to each storm at some point. It does not include
untreated control counties. Each treated county-storm pair is expanded
over the event window from eight periods before to twenty periods after
exposure. In this case the calendar period is designed so, so the event
time is centered on the county's own first exposure to the storm. This
means that period zero in this treated-only panel corresponds to the
first period in which the specific county is affected by the specific
storm. If a storm reaches different counties in different weeks, those
counties are aligned according to their own local exposure dates. The
treated-only panel is therefore county-centered rather than
storm-centered. This makes it useful for describing what happens before
and after exposed counties are first hit by a storm. After expanding the
event window, the actual county-period outcomes and controls (i.e.
nightlight and era5 data) are joined and some indicator variables are
added. The main limitation of this dataset is that it does not contain a
control-group to make valid comparisons against. It is therefore not
well suited for causal interpretation, through e.g. a
difference-in-differences or event-study design that require untreated
controls. It is however well suited for descriptive analysis of treated
counties, \[for example plotting average nighttime lights around the
local exposure date, studying the distribution of exposure intensity and
duration, or checking whether outcomes already move before local storm
exposure.\]

CREATE THE STACK UNITS

The stacked event-study panel is then constructed by crossing every
storm with every county in the analysis panel. County-specific exposure
information is joined to these storm-county combinations. Counties that
appear in the exposure data for a given storm are marked as treated for
that storm. All other counties are retained as controls for that storm.
Thus, control counties are not chosen by matching, distance, state, or
propensity-score criteria. They are simply the counties in the sample
that were not exposed to the relevant stack storm. A county can
therefore be treated in one storm stack and serve as a control in
another.

BUILD EVENT UNITS (INTERMEDIARY OBJECT that combines the ibtracs dfs we
built above) The stacked event study panel is build from an intermediate
object , which starts by crossing every storm with every county in the
analysis panel. This creates a dataset with one row for each
storm-county combination, regardless of whether the county was affected
by that storm. An important mechanisms here is how treatment status is
assigned: this happens very mechanically (no propensity score matching,
etc.). A county is treated in a given storm stack, if it has a matching
county-storm row in the county-specific exposure panel we built above.
If there is no such row (i.e. the given county was not affected by the
given storm) then the county does remain in the panel (in contrast to
the treated-panel above) and is marked as untreated. So control counties
are precisely those counties in the sample that were not exposed to a
particular stack storm. A county can thus be treated in one storm stack
and serve as control in another.

BUILD FINAL STACKED EVENT PANEL The final stacked panel expands the
event-units data over the same event window, from eight periods before
to twenty periods after the storm-level event period. In this panel, the
calendar period is constructed as so that t=0 refers to the first period
in which the storm appears anywhere in the sample. This is
storm-centered event time. After the event window is expanded,
county-period outcomes and controls are joined from the event outcome
base. Rows whose implied calendar period falls outside the available
2016-2018 panel are dropped. This means that event windows can be
unbalanced for storms near the beginning or end of the sample. The panel
then constructs indicators for whether the stack storm is actually
active in the county-period, whether another storm is active, and how
many storms are active in that county-period.

CONSTRUCTING FEs:

The stacked panel also constructs fixed-effect identifiers. The
county-storm fixed effects identifies a county within a storm stack and
can absorb time-invariant differences between counties within each
stack. The storm-period fixed effects identifies a storm stack by
calendar period and can absorb shocks common to all counties in the same
stack-period. These identifiers are used in the stacked event-study
specification.

\[How are treated and control counties chosen in each storm stack?\]
treated counties in a given stack are counties that ever experience that
stack storm. Controls are chosen very mechanically: for each storm, the
code takes all counties in the analysis panel, then marks as treated
only the counties that appear in $storm_event_exposure_df$ for that
storm. So for a stack like MARIA-2017: treated counties = counties with
a GEOID x MARIA-2017 row in $storm_event_exposure_df$ control counties =
all other counties in the sample that do not have a GEOID x MARIA-2017
exposure row The controls are therefore not chosen by matching,
distance, same state, propensity score, etc. They are simply the
complement of treated counties within the full county sample. Important
nuance: a control county for one stack can be treated in another stack.
For example, a Texas county hit by HARVEY-2017 could still be a control
in the MARIA-2017 stack if it was never hit by Maria.

# Modeling Choices

\[why asinh\] Mean nightlight intensity is highly right-skewed and
includes very small or zero values. To reduce the influence of extreme
observations while retaining municipality-weeks with low measured
luminosity, I use the inverse hyperbolic sine transformation of
nightlight intensity as the main outcome. This transformation is
approximately linear near zero and approximately logarithmic for larger
values. As a robustness check, I also estimate specifications using the
log of nightlight intensity plus a small offset.

# Research Design {#design}

## Two-way-fixed-effects base model

As a first set of base specifications, I estimate the relationship
between tropical storm exposure and nighttime light intensity using a
county-week panel design with two-way fixed effects. The dependent
variable is inverse-hyperbolic-sine-transformed nighttime light
intensity, ($ntl_ihs_{it}$), observed for county (i) in week (t). The
simplest specification takes the form
$$ntl_ihs_{it} = \alpha_i + \lambda_t + \beta X_{it} + \varepsilon_{it},$$
where ($\alpha_i$) denotes county fixed effects, ($\lambda_t$) denotes
period fixed effects, and ($X_{it}$) is a measure of storm or weather
exposure in county (i) during period (t). The county fixed effects
absorb time-invariant differences across counties, such as geography,
average economic activity, settlement patterns, or persistent
differences in baseline nightlight levels. The period fixed effects
absorb shocks common to all counties in a given week, such as
seasonality, national economic conditions, satellite-related measurement
changes, or other aggregate temporal patterns. Identification therefore
comes from within-county changes in nightlight intensity that occur in
weeks with higher storm or weather exposure, relative to the same
county's own average and relative to contemporaneous changes in other
counties. I estimate several variants of ($X_{it}$): first, a binary
indicator for whether a storm is occurring in a given county-week;
second, a continuous storm exposure measure based on storm-track and
wind-field information derived from IBTrACS and 'stormR'; and third,
ERA5-based weather variables, namely maximum wind gust, total
precipitation, and a joint specification including both wind gust and
precipitation. In addition to the contemporaneous two-way fixed effects
models, I estimate distributed-lag versions that allow the effect of
storm exposure to evolve dynamically before and after the storm period.
These models can be written as

$$ntl_ihs_{it} = \alpha_i + \lambda_t + \sum_{k=-4}^{20} \beta_k X_{i,t-k} + \varepsilon_{it}$$

where the coefficients ($\beta_k$) trace the association between storm
exposure and nightlight intensity from four weeks before exposure to
twenty weeks after exposure. Negative values of (k) represent leads and
can be interpreted as pre-storm placebo or pre-trend coefficients, while
(k=0) captures the contemporaneous relationship and positive values of
(k) capture lagged post-storm dynamics. For the joint weather model, the
dynamic specification is extended to include both weather dimensions
simultaneously,
$$ntl_ihs_{it} = \alpha_i + \lambda_t + \sum_{k=-4}^{20} \beta_k Wind_{i,t-k} + \sum_{k=-4}^{20} \gamma_k Precipitation_{i,t-k} + \varepsilon_{it}.$$
This framework differs from a conventional staggered
difference-in-differences design because treatment is not an absorbing
state and counties do not experience a single clearly defined treatment
start. Instead, counties may be exposed to multiple storm events over
the sample period, and recovery may occur gradually, with some counties
returning to pre-storm light levels before later storms while others may
be hit again before full recovery. The distributed-lag specification is
therefore useful as a descriptive base model because it summarizes the
average dynamic relationship between repeated storm exposure and
deviations in nighttime lights, while allowing for both immediate
disruptions and delayed recovery patterns. All models are estimated
using observation weights based on the mean high-quality pixel share, so
county-weeks with better valid-pixel coverage receive more weight in the
estimation. Standard errors are clustered at the county level to account
for serial correlation and arbitrary within-county dependence over time.

### Identifying Assumptions

The causal interpretation of the two-way fixed base models relies on a
number of identifying assumptions. Most importantly, parallel trends are
assumed, meaning that in the absence of storm exposure, treated and
untreated county-weeks would have followed similar trends in nighttime
light intensity, after accounting for county fixed effects and period
fixed effects. The fixed effects play an important role here, in
removing any permanent differences across counties and any common shocks
to all counties in a given week. However for causal interpretation any
remaining within-county change in nighttime lights around storm exposure
has to be independent of any other time-varying factors that are
correlated with storm occurrence or intensity. Further identifying
assumptions include the exogeneity of occurrence and storm exposure with
respect to potential nightlight outcomes, again conditional on the fixed
effects and the other included weather measures (if applicable). The
dynamic lead-lag specifications further require not anticipation
effects, meaning that nighttime lights should not already respond before
a county is hit by a storm. They further require limited spillovers
across counties and events, meaning that untreted or less exposed
counties should not indirectly affected by nearby storm damage,
evacuations, power-grid disruptions, emergency responses or economic
displacement from affected neighboring counties. Finally the
distributed-lag model assumes that the estimated coefficients at each
lead and lag can be interpreted as actually clean event-time effects
rather than a mix of different storms, recovery paths and exposure
histories.

These assumptions are unlikely to hold perfectly in the present
application. Although tropical storms can plausibly be treated as
externally generated meteorological shocks, the realized exposure and
economic consequences of storms are not randomly distributed across
counties. Coastal location, settlement patterns, infrastructure quality,
grid resilience, local income, urban density, and disaster preparedness
may all influence both the probability or severity of storm exposure and
the path of nighttime light recovery. County and period fixed effects
account for permanent county differences and common weekly shocks, but
they do not fully address time-varying local vulnerability or
differential trends across more and less storm-prone regions. The
results presented in section XX clearly show that the no-anticipation
assumption is severely violated. The reasons for this likely arise from
increasingly accurate storm forecasts and consequently the considerable
efforts by the United States government, to prevent major damages. This
gives a lot of people the ability to prepare in advance, evacuate, close
businesses, shut down infrastructure, etc. all of which dilutes the
actual storm-impact effect. A further complication arises from the
repeated nature of storm exposure. Unlike a standard treatment setting
with one treatment date, counties in this analysis may be hit multiple
times. Some counties may even experience a new storm before they have
fully recovered from the previous one. As a result, a county-week that
appears to be a control observation for one storm may still be affected
by past storm exposure or may soon be affected by another storm. This
makes it difficult for the pooled TWFE models to cleanly separate
contemporaneous storm effects from ongoing recovery effects.

A further concern is that conventional two-way fixed effects event-study
regressions can be difficult to interpret when treatment timing varies
across units and treatment effects are heterogeneous. In staggered or
repeated treatment settings, already-treated units may implicitly serve
as controls for later-treated units, and later-treated units may serve
as controls for earlier-treated units.

\[why the simple pooled twfe is not a good idea\]
https://ideas.repec.org/a/eee/econom/v225y2021i2p175-199.html
https://ideas.repec.org/a/eee/jfinec/v144y2022i2p370-395.html

A normal TWFE event study is risky when treatment timing differs across
units and effects are heterogeneous. Sun and Abraham show that
conventional lead/lag TWFE coefficients can be contaminated by treatment
effects from other periods, even producing misleading pretrends under
heterogeneous dynamic effects. Baker, Larcker, and Wang make the same
practical warning for staggered DiD applications: standard staggered
regressions can be biased and can change inference materially. Since
storms differ by intensity, location, infrastructure vulnerability, and
recovery trajectory, treatment-effect heterogeneity is almost guaranteed
in your case.

If storm effects differ across events, counties, exposure intensities,
or post-storm horizons, the resulting coefficients may combine many
underlying treatment effects with non-transparent and potentially
problematic weights. This is especially relevant here because storm
impacts are likely to be heterogeneous by storm intensity, local
infrastructure vulnerability, population density, baseline
electrification, and recovery capacity. The lead and lag coefficients
from the pooled TWFE event-study model should therefore be interpreted
primarily as descriptive evidence on average dynamic associations rather
than as causal estimates.

## Stacked Storm Event Study Design

To work around some of the limitations of the previous models the main
specification of this analysis uses a stacked stacked event study around
each storm's exposure period.

This approach uses the event-study data whose preparation was outlined
in the Data section. In short it reorganizes the panel data around
individual storm events and constructs event-specific comparison groups,
essentially treating each storm as its own small event study. Then,
instead of estimating one pooled model across all storms and all
county-weeks, each storm creates a separate event window (i.e. the
periods where the storm was active) in which exposed counties are
compared to a set of control counties that are not exposed to the same
storm during the relevant window. These event-specific datasets are then
stacked and estimated jointly.

This design offers many advantages and helps to address several
weaknesses of the pooled TWFE specification. First, it makes the timing
of treatment more explicit by aligning observations relative to each
storm event. Second, it reduces the risk that already-treated or
not-yet-recovered counties are used as inappropriate controls,
especially if counties with overlapping storm exposure are excluded from
the control group or from the event window. Third, it allows the
researcher to define cleaner pre- and post-storm periods for each event,
making pre-trend checks and dynamic recovery patterns more
interpretable. Fourth, by comparing exposed and unexposed counties
within the same storm-specific context, the design can better account
for event-level shocks and regional conditions surrounding a given
storm. The stacked event-study approach does not eliminate all
identification concerns: it still requires conditional parallel trends
between treated and control counties within each event window, no
substantial spillovers to control counties, limited anticipation
effects, and accurate measurement of storm exposure and nighttime
lights. Nevertheless, it provides a more credible and transparent
framework for estimating dynamic storm impacts in a setting with
repeated, heterogeneous, and potentially overlapping treatment events.

### Main event study specification (binary treatment)

The main specification of this analysis is a stacked-event study model,
estimated on the storm level stacked panel. Mathematically this model
can be written as:

$$y_{ist}=
\alpha_{is}+\lambda_{st}+
\sum_{\substack{k=K_{min} \ k \neq -1}}
\beta_k\left(\mathbf{1}{\tau_{ist}=k} \times D_{is} \right) +
\varepsilon_{ist}.$$

Here, ($Y_{ist}$) denotes the outcome variable, for geography (i), storm
stack (s), and time period (t). The geography index (i) refers to a
geographic unit, such as a county or municipio, the storm index (s)
refers to a specific storm (tropical cyclone) event and the time index
(t) refers to the calendar period in the panel.

The variable $D_{is}$ is an indicator variable equal to one if geography
i is treated in storm stack s. Treated in this case means that the
county was exposed to the corresponding storm. The variable is equal to
zero for clean control geographies in the same storm stack. The variable
$\tau_{ist}$ denotes relative event time. For treated geographies, this
variable is defined relative to the county specific local storm exposure
period. For control geographies, it is defined relative to the
storm-specific event period. Thus, $\tau_{ist}$ denotes the (treatment)
event period, negative values denote pre-event periods and positive
values denote post-event periods.

The coefficients of interest are the $\beta_k$. Each $\beta_k$ measures
the difference in nightlight intensity between treated and control
geographies in relative period (k), compared to the omitted reference
period (which in this case is k=-1), which serves as the baseline.
Coefficients with $k<0$ serve are the pre-trend differences and can be
used to asses the plausibility of the parallel pre-trends assumption
(more on that below). Coefficients with $k>0$ describe the evolution (or
recovery) of the outcome variable after storm exposure. They can be
interpreted as the estimated storm effect in each post-event period
relative to the pre-storm baseline.

There are two fixed effect terms included in the specification above.
The term $\alpha_{is}$ represents county-by-storm fixed effects, aimed
to absorb all time-invariant differences between geographies within each
storm stack, such as baseline luminosity, economic differences, and also
more persistent differences such as in settlement patterns (coastal vs.
inner country matters especially here), infrastructure, etc. The
interaction between counties (or geographies) and storms is necessary to
keep the fixed effects stack-storm specific. The second fixed effects
term $\lambda_{st}$ represents period-by-storm fixed effects, aimed to
absorb all county-invariant differences (i.e. shocks that are common to
all counties) between periods within each storm stack, such as national
or regional economic shocks, satellite-period effects and storm-specific
conditions.

The model is implemented in R using the fixest package, which runs a
weighted least squares regression. Observations are weighted by a
quality flag variable from the blackmarble dataset that gives
high-quality observations higher weights to reduce the influence of
noisy or low-quality satellite observations. Standard errors are
clustered firstly by geography to account for serial correlation within
the same geographic unit over time and across storm stacks and secondly
by storm to account for correlation among observations affected by the
same tropical cyclone event.

In summary, the model above estimates the average dynamic (since we are
looking at the evolution over time) effect of storm exposure on
nightlights by comparing exposed counties to clean control counties
within the same storm-specific event window.

\- One binary to explore the impact of a storm vs no storm - One
continuous to further explore the effect of storm severity

BINARY MODEL

$$\alpha_i$$

geo x storm -\> storm x period

### The weights and trimming

\[Why weighing\] In their paper Wing, freedman and Hollingsworth outline
that ordinary stacked DID regressions can be biased even when the
underlying sub-experiments satisfy the usual DID assumptions, because
the stacked regression implicitly weights treated and control trends
differently. In the stacked data, treated observations from a
sub-experiment are weighted based on their share of all treated
observations and control observations are weighted based on their share
of all control observations. These two shares are usually not the same,
and when the regression pools across the sub-experiments the treated and
control untreated-outcome trends don't cancel out. So even if the common
trends assumption holds in each sub-experiment the pooled stacked
regression can still be biased because it combines the sub-experiments
unevenly.

To mitigate this bias the authors propose to estimate the stacked
event-study regression using a set of corrective sample weights. The
construction of these weights works as follows: First treated
observations always receive weight 1. Control observations in
sub-experiments receive a corrective weight that is equal to the treated
share of that sub-experiment divided by the control share of that
sub-experiment. This simply forces the control trends to be aggregated
across sub-experiments using the same weights as the treated trends. The
calculation of these weights is also conveniently outlined by the
researchers. \[source 18\]

\[Why trimming\] A central insight is that event-study averages can be
misleading when the set of adoption groups contributing to the estimate
changes across event time. For example, early adopters may be observable
many years after treatment, while late adopters may only be observable
for one post-treatment year. If treatment effects differ across groups,
a naive event-time average can look like treatment effects are
dynamically rising or fading, even when each group's own effect is
constant To prevent this, the authors define a fixed event window using
$\kappa$ pre pre-treatment periods and $\kappa$ post post-treatment
periods. An adoption event is kept only if it satisfies two criteria: it
occurs far enough from the beginning and end of the data to observe the
full event window, and it has at least one clean control unit available
through the relevant post-treatment window. Adoption events that are too
early, too late, or lack clean controls are trimmed out.

This trimming is not just a data-cleaning detail. It defines the
estimand. The resulting event-study path is balanced: the same adoption
groups, with fixed weights, contribute at every event time.

The authors propose to construct a balanced \"trimmed\" set of
sub-experiments and

### Identifying Assumptions

\*\* No idea if this is actually true. Check this. The coefficient for
each period k $\beta_k$ estimates an average dynamic treatment effect on
the treated counties. \*\* This effect is assumed to come from the
average difference between treated counties' observed nighttime lights k
periods after storm exposure and the nighttime lights they would have
had at the same period k had the storm not affected them. This
counterfactual scenario, as always in econometrics cannot be observed
and is only approximated by the stacked design by using by using
unexposed counties within each storm-specific sub experiment.

This approach builds on the staggered Difference-in-Differences
framework established by Wing, Freedman, and Hollingsworth \[source
15\]. In their paper they define an aggregate event-time ATT as a
weighted averge of group-time ATTs over a trimmed set of
sub-experiments, where trimming is meant to keep the set of events
balanced across the event window.

To be able to interpret the final coefficients as causal effect of storm
intensity on nightlight brightness a number of assumptions need to hold.

\[Parallel Trends in untreated potential outcomes\]

The central identifying assumption in the stacked event study model is
stack-specific parallel trends. This means that for each storm-stack
(i.e. each storm-specific sub-event study) the treated and the control
counties would have followed the same nightlight-trend in the absence of
each stack's storm.

Let $s$ index a storm, $i$ a county and $t_s$ the event week for storm s
(i.e. the week where the storm hit a certain county) and relative period
k

\[Note to myself, should this be $t_{is}$ for the model where I use the
combi_rel_period period?\]

Also $D_{is}=1$ if county i is treated/exposed in storm stack s. Let
$Y_{it}(0)$ be the untreated potential nightlights. Mathematically the
stack-specific parallel trends assumption can be written as
$$E[Y_{is,k}(0)-Y_{is,-1}(0)|D_{is}=1, s]=E[Y_{is,k}(0)-Y_{is,-1}(0)|D_{is}=0, s]$$.

\[No anticipation / pre-treatment storm effects\] The second key
assumption is that the storm does not affect nightlight before the
event-treatment period - the causal effect of treatment should be zero
in the pre-treatment periods. Concretely this means that counties should
not already experience systematic nightlight changes before the storm
exposure period. This assumption is only plausible for truly unexpected
storm impacts and similarly to the base models it is not very likely to
hold in this setting. The main problem could arise from the choosing
period -1 as the reference period. If this period is already affected by
storm preparatino or weather conditions then all post-treatment
coefficients are measured against a \"contaminated\" baseline. For this
reason the robustness table includes a model with shifted baseline
periods.

\[Clean controls and no contamination by other storms\] In addition to
the two key identifying assumptions treatment has to be unique and only
affect treated counties. When working with the full dataset this is not
plausible. There are multiple instances of multiple storms intersecting
or following right after each other. To make sure these contaminated
data does not enter the final estimation, any county-periods (i.e. rows)
that experience more than one storm are removed from the
stacked-event-study dataset.

A concern that does remain after this cleaning is that the data only
maps direct exposure to the event storms. This may not always mean that
a county was truly untreated in the sense that there were no storm
effects. A control county that is nearby to the treatment county could
be affected indirectly through power-grid interconnections, displaced
economic activity, supply-chain disruptions, evacuation inflows, etc. It
is incredibly difficult to control for all these spillover effects. Fine
scale data on economic activity, (short-term) migration flows, etc.
could significantly improve the validity of the design here.

\[Note on periods\]

The more periods we consider after the storm first hit, the more diluted
the results become - sometimes even with other storms. That means the
long-run coefficients are partly comparing a different set of storms
than the short-run coefficients.

### Alternative Model specifications and robustness checks

\[Grouped periods\] - There are pretty clear anticipatory effects - Use
cleaner multi-period baseline

\[continuous treatment\] Your binary treatment is "storm exposure." That
is clean and interpretable, but it compresses a lot of heterogeneity:
wind speed, rainfall, duration, flooding, storm surge, cloud cover, and
infrastructure vulnerability. The identifying assumption is not that all
storms have the same effect. Event-study DiD can allow heterogeneous
effects. But it does require that the treatment contrast is well
defined: treated counties are meaningfully exposed, controls are
meaningfully unexposed, and relative event time is correctly aligned.

\[Binned continuous data\] - Binned effects

\[Measurement Error\] As with every empirical study this analysis stands
and falls with the validity of the data. Nighttime lights, while
offering a lot of advantages, can be tricky to work with, especially
when research wether related effects. causal identification also
requires that observed NTL changes reflect real changes in
luminosity/economic activity/electricity service rather than purely
sensor or weather artifacts. NASA's Black Marble algorithm is designed
to produce cloud-free, atmospherically corrected nighttime radiance and
reduce background noise at short time scales. Additionally the use of
the quality flags as weights and robusteness checks with the quality
flags show no different effect.

\[changing storm composition\] Wing et al. emphasize that aggregated
event-study coefficients can be misleading if the set of underlying
group-time effects changes over event time. Their paper's "trimmed
aggregate ATT" is designed so that the same sub-experiments contribute
across the event window; otherwise, changes in the event-study path may
reflect changing composition rather than recovery dynamics.

I checked your attached CSV after applying your filters. The filtered
data have 349,060 rows, 21 storm names, 609 unique GEOIDs, and relative
periods from -9 to 20. But support is not perfectly balanced. Some
storms have only 20 or 25 relative periods after filtering, while others
have 29 or 30. Also, some storm-by-relative-period cells have no treated
observations or no control observations. Period -9 is especially
notable: after your filters, it contains treated observations only and
no controls.

That does not automatically invalidate the regression, but it does
weaken the clean "recovery path" interpretation. Later lags, early
leads, or periods affected by dropped overlapping storms may be
estimated from a different mix of storms than central periods. You
should either trim to a balanced window or be very explicit that
coefficients at some relative periods are based on changing composition.

\[ERA5 validation checks\] As a validation exercise, I estimate stacked
event studies using ERA5 precipitation and wind gusts as outcomes.
Treated geographies display sharp increases in storm-related weather
around the event date, supporting the interpretation that the treatment
indicator captures physical cyclone exposure."

"I exclude control observations with extreme ERA5 rainfall or wind gusts
to ensure that control geographies are not experiencing other severe
weather shocks during the event window."

\[What am I doing with the ERA5 data?\] ERA5 variables are not included
as main controls because contemporaneous wind and precipitation are part
of the tropical cyclone treatment itself. Controlling for them would
remove part of the causal pathway from storm exposure to nightlight
disruption and would therefore change the estimand from the total effect
of storm exposure to a conditional effect net of measured weather
intensity. Instead, I use ERA5 data for validation and robustness: to
confirm that treated geographies experienced stronger wind and rainfall
around the event, to assess pre-treatment balance between treated and
control units, to exclude control observations affected by other severe
weather, and to examine whether nightlight losses are larger under
higher physical storm intensity.

\[Standard error - wild cluster bootstrap\]

\- standard errors potentially underestimated - use wild cluster
bootstrap

# Results

## Main Table: Distributed-lag panel model

## Main Figure: Stacked Event Study

"The average stacked estimate shows a decline in nightlights after storm
exposure, but the effect is concentrated in the most severe storms,
especially Maria and Irma. Results are weaker when those storms are
excluded."

## Robustness

### Robustness with ERA5

ERA5 variables are not included as main controls because contemporaneous
wind and precipitation are part of the tropical cyclone treatment
itself. Controlling for them would remove part of the causal pathway
from storm exposure to nightlight disruption and would therefore change
the estimand from the total effect of storm exposure to a conditional
effect net of measured weather intensity. Instead, I use ERA5 data for
validation and robustness: to confirm that treated geographies
experienced stronger wind and rainfall around the event, to assess
pre-treatment balance between treated and control units, to exclude
control observations affected by other severe weather, and to examine
whether nightlight losses are larger under higher physical storm
intensity.

CHECK FOR CLEAR TREATMENT DEFINITION WITH ERA5 As a validation exercise,
I estimate stacked event studies using ERA5 precipitation and wind gusts
as outcomes. Treated geographies display sharp increases in
storm-related weather around the event date, supporting the
interpretation that the treatment indicator captures physical cyclone
exposure

SHOW WEATHER PRE TRENDS WITH ERA5 strengthens your discussion of
stack-specific parallel trends. If treated locations already have
systematically different pre-storm rainfall, wind, or cloud-free shares,
that is a concern

EXCLUDE PERIODS WITH OTHER SEVERE WEATHER EXPOSURE

Even if a control county is not hit by the focal tropical cyclone, it
may experience severe rainfall or wind from another system. Those
observations are bad controls for a storm-recovery design. I exclude
control observations with extreme ERA5 rainfall or wind gusts to ensure
that control geographies are not experiencing other severe weather
shocks during the event window.

# Conclusion

A concise summary of contents and results

The results form this analysis provide the necessary statistical proof
that the data assembled during this analysis can be used as a strong
base to further investigate fascinating economic questions, like how
does the grade of urbanization influence recovery after storms? what
about demographic variables, which role does disaster aid, government
relief programs and in general governance and politics play?

# Appendix {#app}

Additional material goes here

# StormR wind models

Yes --- using Willoughby because it is the StormR default is a valid
starting point, but for a thesis you should justify it more strongly
than "it was the default." The better justification is: \*\*Willoughby
is StormR's default because it gives a flexible empirical radial wind
profile, fitted to aircraft observations, and can be combined with Chen
asymmetry to account for storm motion.\*\*

StormR implements three wind models: \*\*Holland (1980)\*\*,
\*\*Willoughby et al. (2006)\*\*, and \*\*Boose et al. (2004)\*\*. In
StormR, Holland and Willoughby are originally symmetric models, while
Boose is already asymmetric. StormR's default is \*\*Willoughby + Chen
asymmetry\*\*. (\[cran.r-project.org\]\[1\])

\## Big-picture comparison

\| Model \| Core idea \| Symmetry \| Main inputs in StormR \| Best
intuition \| \| -------------------- \|
------------------------------------------------------------ \|
--------------------------------------: \|
--------------------------------------------------------------- \|
----------------------------------------------------- \| \|
\*\*Holland\*\* \| Pressure-based analytical vortex model \| Symmetric
unless Chen/Miyazaki is added \| Needs wind, RMW, central pressure,
outer pressure \| A classic pressure-wind hurricane model \| \|
\*\*Willoughby\*\* \| Empirical wind-profile model fitted to aircraft
observations \| Symmetric unless Chen/Miyazaki is added \| Mainly wind,
RMW, latitude, distance \| A flexible observed-shape wind profile \| \|
\*\*Boose / HURRECON\*\* \| Asymmetric modification of Holland with
land/water friction \| Asymmetric by construction \| Needs wind, RMW,
central pressure, outer pressure, surface type \| A Holland-type model
adapted for surface wind impacts \|

\## 1. Holland model

The \*\*Holland model\*\* is probably the most classic parametric
hurricane wind model. In StormR, it is described as a model based on
\*\*gradient wind balance in mature tropical cyclones\*\*. The wind
distribution is calculated from a circular pressure field using central
pressure, outer closed-isobar pressure, and radius of maximum wind.
(\[cran.r-project.org\]\[1\])

In plain language, Holland starts from the storm's \*\*pressure
structure\*\* and uses that to infer the wind field. Lower central
pressure relative to the surrounding environment generally implies
stronger winds. The model has a shape parameter, often called the
Holland (B) parameter, which controls how sharply winds peak around the
eyewall. (\[cran.r-project.org\]\[1\])

\### Advantages of Holland

\* It is \*\*well established\*\* and widely used in hurricane risk,
storm-surge, and wind-field applications. \* It has a clear physical
interpretation because it links pressure gradients to wind speed. \* It
can be useful when reliable central and environmental pressure data are
available. \* In StormR, it can be combined with \*\*Chen\*\* or
\*\*Miyazaki\*\* asymmetry, so it does not have to remain purely
symmetric. (\[R Documentation\]\[2\])

\### Disadvantages of Holland

\* In its original form, it is \*\*symmetric\*\*, so all points at the
same distance from the eye receive the same wind speed unless an
asymmetry correction is added. \* It requires pressure variables. StormR
notes that 'pres' and 'poci' are mandatory for Holland and Boose, which
can be a practical limitation if those fields are missing or
inconsistent in the storm-track data. (\[umr-amap.github.io\]\[3\]) \*
The radial wind profile can be too simple for some storms. Willoughby et
al. specifically developed their later model partly because common
continuous wind-profile functions showed systematic errors when compared
with aircraft observations. (\[cir.nii.ac.jp\]\[4\])

\### Thesis-friendly interpretation

\> The Holland model is a classic analytical pressure-wind model. It is
attractive because it has a clear physical basis and is widely used, but
it depends on pressure information and produces a symmetric wind field
unless an asymmetry correction is added.

\## 2. Willoughby model

The \*\*Willoughby et al. (2006)\*\* model is different from Holland
because it is more directly focused on representing the \*\*radial wind
profile\*\* itself. StormR describes it as an empirical model fitted to
aircraft observations. It separates the storm into an inner region,
where wind increases toward the radius of maximum wind, and an outer
region, where wind decays exponentially away from the eyewall.
(\[cran.r-project.org\]\[1\])

The important intuition is that Willoughby is designed to better capture
the observed shape of hurricane winds: weak near the center, strongest
near the eyewall, and then declining outward. The original Willoughby et
al. paper describes the model as a family of sectionally continuous
profiles in which wind increases as a power function inside the eye and
decays exponentially outside the eye, with stronger hurricanes sometimes
requiring two exponential decay components outside the eyewall.
(\[cir.nii.ac.jp\]\[4\])

\### Advantages of Willoughby

\* It is \*\*empirically fitted to aircraft observations\*\*, which
makes it attractive when the goal is to reconstruct realistic radial
wind profiles. \* It has a more flexible radial structure than a simple
pressure-based model. \* It explicitly treats the inner and outer wind
regions differently. \* In StormR, it is the \*\*default model\*\*, and
the default setting combines it with \*\*Chen asymmetry\*\*.
(\[cran.r-project.org\]\[1\]) \* It does not require 'pres' and 'poci'
in the same way Holland and Boose do; StormR states those pressure
fields are mandatory for Holland and Boose, while RMW is highly
recommended more generally. (\[umr-amap.github.io\]\[3\])

\### Disadvantages of Willoughby

\* The basic Willoughby model is still \*\*symmetric\*\*. It only
becomes asymmetric in StormR if Chen or Miyazaki asymmetry is added.
(\[cran.r-project.org\]\[1\]) \* It depends strongly on (R_m), the
radius of maximum wind. If RMW is missing, noisy, or empirically
estimated, the modeled wind footprint can change meaningfully. \* It
does not explicitly treat land and water differently. Surface friction
effects are not built into the model the way they are in Boose. \* It is
still a parametric simplification, not a full atmospheric simulation.

\### Thesis-friendly interpretation

\> The Willoughby model is useful because it provides an empirically
based representation of the radial wind structure of tropical cyclones.
It is particularly suitable when the objective is to estimate wind
exposure from storm-track data, because it reconstructs the wind profile
from a limited set of storm characteristics while allowing asymmetry to
be added separately.

\## 3. Boose model / HURRECON

The \*\*Boose et al. model\*\*, also known as \*\*HURRECON\*\*, is an
asymmetric modification of the Holland model. StormR describes it as a
modification of Holland that adds asymmetry and treats land and water
differently using different surface-friction coefficients. (\[R
Documentation\]\[2\])

This is the key point: \*\*Boose is not simply "Willoughby with
asymmetry."\*\* It is closer to \*\*Holland with built-in asymmetry and
land/water friction treatment\*\*.

The HURRECON model is designed to estimate hurricane surface wind speed
and direction from hurricane track, size, intensity, and surface type.
The Harvard Forest documentation describes HURRECON as a model based on
track, size, intensity, and land/water surface type, originally intended
for estimating wind speed, direction, and damage patterns.
(\[harvardforest1.fas.harvard.edu\]\[5\])

\### Advantages of Boose

\* It is \*\*asymmetric by construction\*\*, so you do not need to
choose Chen or Miyazaki separately. \* It includes a simple treatment of
\*\*surface friction\*\*, distinguishing between land and water. In
StormR, the friction scaling parameter is listed as (F = 1.0) over water
and (F = 0.8) over land. (\[cran.r-project.org\]\[1\]) \* It is useful
for impact-oriented studies because HURRECON was developed for
estimating surface wind fields and hurricane disturbance/damage
patterns. (\[harvardforest1.fas.harvard.edu\]\[5\]) \* For county-level
land exposure, the land/water distinction may be conceptually
attractive.

\### Disadvantages of Boose

\* It is based on Holland, not Willoughby. So choosing Boose changes the
radial wind model, not just the asymmetry treatment. \* It requires
pressure variables in StormR. As noted above, 'pres' and 'poci' are
mandatory for Holland and Boose. (\[umr-amap.github.io\]\[3\]) \* The
land/water friction treatment is simple. A county is not just "land" in
a meteorological sense; real surface roughness varies across forests,
cities, wetlands, mountains, cropland, and coastlines. \* If your goal
is to follow StormR's default structure, Boose moves you away from that
default Willoughby-based workflow.

\### Thesis-friendly interpretation

\> The Boose model is useful when asymmetry and surface friction are
central to the research design. However, using Boose does not merely add
asymmetry to the Willoughby model; it changes the underlying
wind-profile formulation to a Holland-based model and introduces
additional assumptions about land-water friction.

\## Main conceptual differences

\### A. Holland and Boose are more pressure-based; Willoughby is more
wind-profile-based

Holland and Boose rely on the pressure deficit between the storm center
and the outer closed isobar. Willoughby instead directly parameterizes
the radial wind profile using maximum wind, RMW, latitude, and empirical
shape parameters. (\[cran.r-project.org\]\[1\])

For your thesis, this matters because IBTrACS data availability and
consistency may influence which model is easiest and most defensible to
use. If pressure fields are missing or inconsistent, Holland and Boose
become harder to apply consistently.

\### B. Holland and Willoughby are symmetric unless adjusted

In StormR, both Holland and Willoughby originally produce symmetric wind
fields around the cyclone center. StormR allows asymmetry to be added
using Chen or Miyazaki, both of which adjust the symmetric wind field
using storm translation speed. (\[cran.r-project.org\]\[1\])

Boose already includes asymmetry, so it does not use the same 'asymmetry
= \"Chen\"\` or 'asymmetry = \"Miyazaki\"\` structure.

\### C. Boose includes land/water friction; Holland and Willoughby do
not in the same way

This is one of Boose's biggest practical differences. StormR states that
Boose treats land and water differently through different
surface-friction coefficients, whereas Holland and Willoughby do not
have that built-in land/water distinction. (\[cran.r-project.org\]\[1\])

That can be an advantage for damage or exposure studies, but it is also
an added assumption.

\## Which model is best for your thesis?

For your use case --- \*\*estimated county-level wind exposure in the
U.S. using IBTrACS and StormR\*\* --- I would not say one model is
universally "best." I would frame it like this:

\| Research priority \| Model that fits best \| \|
------------------------------------------------------------------ \|
--------------------------------------- \| \| Follow StormR default and
use an empirically observed wind profile \| \*\*Willoughby + Chen\*\* \|
\| Use a classic pressure-wind model with broad precedent \|
\*\*Holland + Chen\*\* \| \| Include built-in asymmetry and land/water
friction \| \*\*Boose\*\* \| \| Minimize dependence on pressure
variables \| \*\*Willoughby\*\* \| \| Emphasize surface wind impacts
over land \| \*\*Boose\*\*, possibly as robustness check \|

My honest recommendation: \*\*keep Willoughby + Chen as your main
specification\*\*, but do not justify it only by saying it is the
default. Justify it like this:

\> I use the Willoughby et al. (2006) model because it is StormR's
default wind-profile model and provides an empirical representation of
the radial wind structure of tropical cyclones based on aircraft
observations. In contrast to the Holland model, which derives wind speed
from an idealized pressure field, the Willoughby model directly
parameterizes the increase of wind speed toward the eyewall and the
decay of wind speed outside the radius of maximum wind. I apply the Chen
asymmetry correction to account for storm translation, following
StormR's default model configuration.

Then, if you want to be extra solid, add a robustness sentence:

\> As a robustness check, estimates can be compared with alternative
StormR specifications, such as Holland + Chen or Boose, since these
models differ in their treatment of pressure structure, asymmetry, and
surface friction.

\## Short advantages/disadvantages summary

\### Holland

\*\*Advantages:\*\* classic, widely used, physically interpretable,
pressure-gradient based. \*\*Disadvantages:\*\* symmetric unless
adjusted, needs pressure data, simpler radial structure, may fit
observed wind profiles less well in some cases.

\### Willoughby

\*\*Advantages:\*\* empirical, aircraft-observation based, flexible
inner/outer wind profile, StormR default, well suited for radial wind
reconstruction. \*\*Disadvantages:\*\* symmetric unless adjusted,
sensitive to RMW, no explicit land/water friction, still simplified.

\### Boose

\*\*Advantages:\*\* asymmetric by design, includes land/water friction,
useful for surface impact studies. \*\*Disadvantages:\*\* Holland-based
rather than Willoughby-based, requires pressure data, uses simplified
friction assumptions, changes multiple modeling assumptions at once.

For your thesis, the cleanest defense is: \*\*Willoughby + Chen is a
reasonable main model because it uses StormR's default empirically based
radial wind profile while still accounting for storm-motion
asymmetry.\*\*

\[1\]:
https://cran.r-project.org/web/packages/StormR/vignettes/Models.html
\"Models\" \[2\]: https://rdrr.io/cran/StormR/man/spatialBehaviour.html
\"spatialBehaviour: Computing wind behaviour and summary statistics over
given\... in StormR: Analyzing the Behaviour of Wind Generated by
Tropical Storms and Cyclones\" \[3\]:
https://umr-amap.github.io/StormR/reference/storm-class.html \"storm
object --- storm-class • StormR\" \[4\]:
https://cir.nii.ac.jp/crid/1363951795028323712 \" Parametric
Representation of the Primary Hurricane Vortex. Part II: A New Family of
Sectionally Continuous Profiles \| CiNii Research \" \[5\]:
https://harvardforest1.fas.harvard.edu/exist/apps/datasets/showData.html?id=hf025
\"Data Archive\"

Assuming you mean \*\*tropical-cyclone / hurricane wind-field
models\*\*, the core difference is this:

\*\*Holland (1980)\*\* is a \*\*physics-based, symmetric pressure--wind
model\*\*. It assumes a circular storm and uses gradient-wind balance:
wind speed is derived from the pressure field using central pressure,
environmental pressure, radius of maximum wind, latitude/Coriolis, and a
shape parameter. By itself, it gives a radially symmetric vortex, so
storm-motion asymmetry has to be added separately.
(\[cran.r-project.org\]\[1\])

\*\*Willoughby et al. (2006)\*\* is a \*\*data-fitted, symmetric
wind-profile model\*\*. Instead of deriving winds from a pressure
profile like Holland, it directly fits the radial wind profile to
aircraft observations. It treats the storm in sections: winds increase
inside the eye, transition smoothly across the eyewall, and then decay
outside using exponential terms. It is usually better at representing
observed inner-core and outer-wind structure, but it is still symmetric
unless asymmetry is added separately. (\[cran.r-project.org\]\[1\])

\*\*Boose et al. (2004)\*\*, also known as \*\*HURRECON\*\*, is an
\*\*asymmetric modification of the Holland model\*\*. It builds in
asymmetry from storm forward motion and also treats \*\*land and water
differently\*\* through different friction/scaling coefficients. That
makes it especially useful for reconstructing historical hurricane
impacts over landscapes or islands, not just representing an idealized
radial wind profile over open water. (\[cran.r-project.org\]\[1\])

\| Model \| Main idea \| Symmetry \| Key strength \| Main limitation \|
\| ---------------------------- \|
------------------------------------------------------------- \|
-------------------: \|
--------------------------------------------------------------------- \|
-------------------------------------------------------------------------------
\| \| \*\*Holland (1980)\*\* \| Analytical pressure--wind model based on
gradient-wind balance \| Symmetric by default \| Simple, widely used,
needs relatively few parameters \| Can oversimplify real storm structure
and asymmetry \| \| \*\*Willoughby et al. (2006)\*\* \| Empirical
wind-profile model fitted to aircraft observations \| Symmetric by
default \| More realistic radial wind profile, especially near/away from
eyewall \| Needs external method for asymmetry and surface effects \| \|
\*\*Boose et al. (2004)\*\* \| HURRECON: modified Holland model for
hurricane exposure \| Asymmetric by design \| Includes storm motion and
land/water friction differences \| More application-specific; still
simplified compared with full numerical models \|

In plain terms: \*\*Holland is the classic idealized pressure-based
vortex; Willoughby is a more observation-fitted radial wind profile;
Boose is a Holland-derived model adapted for asymmetric, land-impact
reconstruction.\*\* Parametric wind models are widely used because they
are computationally efficient, but no single one is best for every storm
or application. (\[sciencedirect.com\]\[2\])

\[1\]:
https://cran.r-project.org/web/packages/StormR/vignettes/Models.html
\"Models\" \[2\]:
https://www.sciencedirect.com/science/article/pii/S235248552200024X
\"Research progress on tropical cyclone parametric wind field models and
their application - ScienceDirect\"

# Electronic appendix {#el_app}

Data, code and figures are provided in electronic form.

**Declaration of authorship**

I hereby declare that the report submitted is my own unaided work. All
direct or indirect sources used are acknowledged as references. I am
aware that the Thesis in digital form can be examined for the use of
unauthorized aid and in order to determine whether the report as a whole
or parts incorporated in it may be deemed as plagiarism. For the
comparison of my work with existing sources I agree that it shall be
entered in a database where it shall also remain after examination, to
enable comparison with future Theses submitted. Further rights of
reproduction and usage, however, are not granted here. This paper was
not previously presented to another examination board and has not been
published.\
[Location, date]{style="color: orange"}\

------------------------------------------------------------------------

\
[Name]{style="color: orange"}
