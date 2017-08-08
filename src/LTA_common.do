global datapath "D:\Dropbox\LTA\Data"
global envpath "D:\Dropbox\work\data\environment"
global ezpath "H:\ezlink data"
global tmp "H:\tmp"

**********************************************************************
** prepare location classification data
capture: program drop prepare_location
program define prepare_location
	import excel "${datapath}\locations.xlsx", sheet("Sheet1") firstrow clear
	save "${tmp}\location.dta", replace

	drop if strpos(stop_stn, "MRT")
	bys location: keep if _n == 1
	save "${tmp}\stn_of_location.dta", replace
end

**********************************************************************
** prepare weather data
capture: program drop prepare_daily_weather
program define prepare_daily_weather
	use "${envpath}\NUS_hourlyWeather.dta" if hour >=8 & hour <= 20, clear
	gen  temperature_6to10 = temperature if hour >= 6 & hour <= 10
	collapse (max) temperature_max = temperature ///
		(min) temperature_min = temperature ///
		(mean) temperature temperature_6to10 humidity wind_speed (sum) rainfall, by(date)
	save "${tmp}\env.dta", replace
end


**********************************************************************
** prepare weather station coordinate
capture: program drop prepare_weather_stn_coord
program define prepare_weather_stn_coord
	import excel "${datapath}\3G Stns Lat_long_alt.xlsx", sheet("Sheet1") firstrow clear
	gen Stn = substr(Location, 2, .)
	destring Stn, replace
	rename G lon
	keep Stn lat lon
	save "${tmp}\weatherstn_coord.dta", replace
end

capture: program drop prepare_mrt_coord
program define prepare_mrt_coord
	import excel "${datapath}\mrtcoord.xls", firstrow clear
	rename latitude lat
	rename longitude lon
	save "${tmp}\mrtcoord.dta", replace
end

capture: program drop prepare_bus_stop_coord
program define prepare_bus_stop_coord
	* new bus stops
	import delimited D:\Dropbox\work\script\busstops.csv, clear 
	rename latitude lat
	rename longitude lon
	rename busstopcode stop_stn
	replace stop_stn = subinstr(stop_stn, "N", "", .)
	destring stop_stn, replace force
	tostring stop_stn, replace
	rename description LOC_DESC
	tempfile newstops
	save "`newstops'"

	* old bus stops
	use "${datapath}\busCoord2016.dta", clear
	rename BUS_STOP_N stop_stn
	bys stop_stn: gen dup = _N
	destring stop_stn, force replace
	tostring stop_stn, replace
	drop if dup > 1 & LOC_DESC == ""
	drop if inlist(stop_stn , "NIL", "UNK")
	collapse (mean) lat lon (first) id BUS_ROOF_N LOC_DESC, by(stop_stn)
	merge 1:1 stop_stn using "`newstops'"
	drop _merge
	
	save "${tmp}\bus_stop_coord.dta"
end

capture: program drop check_file_exists
program define check_file_exists
	syntax using/, run(string)
	capture confirm file "`using'"
	if _rc==0 {
		di "File `using' exists, skip running `run'"
	} 
	else {
		di "File `using' not exist, run `run'"
		`run'
	}
end


**********************************************************************
** Match bus station with weather station of nearest distance
capture: program drop prepare_nearest_weatherstn
program define prepare_nearest_weatherstn
	check_file_exists using "${tmp}\bus_stop_coord.dta", run(prepare_bus_stop_coord)

	check_file_exists using "${tmp}\mrtcoord.dta", run(prepare_mrt_coord)
	check_file_exists using "${tmp}\weatherstn_coord.dta", run(prepare_weather_stn_coord)
		
	use "${tmp}\bus_stop_coord.dta", replace
	merge 1:1 stop_stn using "${tmp}\mrtcoord.data"
	drop _merge
	
	geonear stop_stn lat lon using "${tmp}\weatherstn_coord.dta", n(Stn lat lon) near(4)
	rename nid1 Stn
	rename nid2 Stn2
	rename nid3 Stn3
	rename nid4 Stn4
	keep stop_stn Stn Stn2 Stn3 Stn4
	save "${tmp}\nearest_weatherstn.dta", replace
end


**********************************************************************
** prepare rain data
capture: program drop prepare_hourly_rain
program define prepare_hourly_rain
	import excel "${datapath}\14station10-122015.xlsx", sheet("Sheet1") firstrow clear
	tempfile rain2015temp
	save "`rain2015temp'"

	import excel "${datapath}\14station10-122016.xlsx", sheet("Sheet1") firstrow clear
	append using "`rain2015temp'"

	gen date = mdy(Mon, day, Year)
	rename Hour hour

	rename Amount rain
	rename Duration rain_duration

	drop Stname

	drop Mon day Year
	save "${tmp}\rain_hh.dta", replace
end


capture: program drop prepare_daily_rain
program define prepare_daily_rain
	prepare_hourly_rain
	keep if hour >= 8 & hour <= 20
	collapse (sum) rain, by(date Stn)
	save "${tmp}\rain.dta", replace
end


capture: program drop prepare_average_daily_rain
program define prepare_average_daily_rain
	check_file_exists using "${tmp}\rain.dta", run(prepare_daily_rain)
	
	use "${tmp}\rain.dta", clear
	drop if rain < 0
	collapse (mean) rain, by(date)
	save "${tmp}\average_rain.dta", replace
end



**********************************************************************
** prepare PM 2.5 data

capture: program drop prepare_hourly_pm25
program define prepare_hourly_pm25
	use "${envpath}\SingaporeHourlyPm25.dta", clear
	egen pm25mean = rmean(central25 east25 north25 south25 west25)
	save "${tmp}\pm25_hh.dta", replace
end

capture: program drop prepare_daily_pm25
program define prepare_daily_pm25
	prepare_hourly_pm25
	keep if hour >= 8 & hour <= 20
	collapse (mean) pm25mean, by(date)
	save "${tmp}\pm25.dta", replace
end



**********************************************************************
** prepare electricity data

capture: program drop prepare_daily_electricity
program define prepare_daily_electricity
	use date endtime systemdemand using "${datapath}\emaelectricity.dta" , clear
	gen hour = hh(endtime)
	keep if hour >= 8 & hour <= 20
	rename systemdemand electricity
	collapse (sum) electricity, by(date)
	save "${tmp}\electricity.dta", replace
end


**********************************************************************
** prepare wind direction data (for fire activity)

capture: program drop prepare_8am_wind
program define prepare_8am_wind
	use date wind_direaction hour if hour == 8 using "${envpath}\NUS_hourlyWeather", clear
	rename wind_direaction winddir
	save "${tmp}\wind.dta", replace
end


***********************************************************************
** prepare fire activity data
capture: program drop prepare_daily_fire
program define prepare_daily_fire
	check_file_exists using "${tmp}\wind.dta", run(prepare_8am_wind)

	import delimited "${datapath}\fire_archive_M6_14049.csv", clear

	gen int date = date(acq_date, "YMD")
	format date %td

	local Singlon 103.801285
	local Singlat 1.368855

	vincenty `Singlat' `Singlon' latitude longitude, hav(distance) replace inkm

	local phi1 = `Singlat'*_pi/180
	gen phi2 = latitude*_pi/180
	gen dlambda = longitude - `Singlon'*_pi/180
	gen bearing_rad = atan2(sin(dlambda)*cos(phi2), cos(`phi1')*sin(phi2) - sin(`phi1')*cos(phi2)*cos(dlambda))
	gen bearing = bearing_rad*180/_pi + 180
	drop phi2 dlambda bearing_rad

	merge m:1 date using "${tmp}\wind.dta", keep(3)

	gen wind_bearing = abs(winddir - bearing)
	replace wind_bearing = 360 - wind_bearing if wind_bearing >= 180

	// inverse distance weight
	gen idw = 1/distance

	// direction difference weight
	gen ddw = cos(wind_bearing*_pi/180)*(wind_bearing<90)

	gen hotspot = 1
	gen hotspot_50 = confidence >= 50
	gen brightness_50 = brightness if confidence >= 50
	gen frp_50 = frp if confidence >= 50

	local fire_mearures hotspot brightness bright_t31 frp hotspot_50 brightness_50 frp_50

	foreach var in `fire_mearures' {
		gen idw_`var' = idw*`var'
		gen ddw_`var' = ddw*`var'
		gen idwddw_`var' = idw*ddw*`var'
	}

	collapse (count) idw ddw (sum) frp* hotspot* bright* idw_* ddw_* idwddw_*, by(date)
	save "${tmp}\fire.dta", replace
end


capture: program drop merge_daily_weather_data
program define merge_daily_weather_data
	check_file_exists using "${tmp}\stn_of_location.dta", run(prepare_location)
	check_file_exists using "${tmp}\nearest_weatherstn.dta", run(prepare_nearest_weatherstn)
	check_file_exists using "${tmp}\rain.dta", run(prepare_daily_rain)
	check_file_exists using "${tmp}\average_rain.dta", run(prepare_average_daily_rain)
	check_file_exists using "${tmp}\wind.dta", run(prepare_8am_wind)
	check_file_exists using "${tmp}\env.dta", run(prepare_daily_weather)
	check_file_exists using "${tmp}\pm25.dta", run(prepare_daily_pm25)
	check_file_exists using "${tmp}\fire.dta", run(prepare_daily_fire)
	
	use "${tmp}\env.dta", clear

	merge m:1 date using "${tmp}\pm25.dta", keep(match master)
	drop _merge

	merge m:1 date using "${tmp}\fire.dta", keep(match master)
	drop _merge

	merge m:1 date using "${tmp}\electricity.dta", keep(match master)
	drop _merge

	merge m:1 date using "${tmp}\wind.dta", keep(match master)
	drop _merge

	merge m:1 date using "${tmp}\average_rain.dta", keep(match master)
	drop _merge
	
	save "${tmp}\daily_merge_env.dta", replace
end


capture: program drop merge_hourly_weather_data
program define merge_hourly_weather_data
	check_file_exists using "${tmp}\stn_of_location.dta", run(prepare_location)
	check_file_exists using "${tmp}\nearest_weatherstn.dta", run(prepare_nearest_weatherstn)
	check_file_exists using "${tmp}\rain_hh.dta", run(prepare_hourly_rain)
	check_file_exists using "${tmp}\pm25_hh.dta", run(prepare_hourly_pm25)
	
	use "${envpath}\NUS_hourlyWeather.dta", clear

	merge m:1 date hour using "${tmp}\pm25_hh.dta", keep(match master)
	drop _merge
	
	save "${tmp}\hourly_merge_env.dta", replace
end


// capture: program drop merge_hourly_weather_riders_by_stop
// program define merge_hourly_weather_riders_by_stop
// 	check_file_exists using "${tmp}\hourly_merge_env.dta", run(merge_hourly_weather_data)
	
// 	local filelist: dir "${ezpath}\newalighting" files "*.dta", respectcase
// 	clear
// 	foreach filename in `filelist' {
		
// 	}
// end

capture: program drop cutandlabel
program define cutandlabel
	syntax varname, at(numlist ascending min=2) gen(name) label(string) [keepbase]
	local level = 0
	foreach upper in `at' {
		if `level' > 0 {
			gen byte `gen'`level' = `varlist' >= `lower' & `varlist' < `upper'
			label var `gen'`level' "`label' in (`lower',`upper')"
		}
		local lower = `upper'
		local level = `level' + 1
	}

	if "`keepbase'" == "" {
		drop `gen'1
	}
end


capture: program drop gen_vars
program define gen_vars
	format date %td

	gen numOfRidersLog = ln(numOfRiders)
	gen electricityLog = ln(electricity)

	gen byte dow = dow(date)
	gen byte month = month(date)
	gen int year = year(date)
	gen byte day = day(date)
	gen int week = week(date)
	gen int ym = ym(year, month)
	gen int yw = yw(year, week)


	gen byte holiday = inlist(date, date("10/11/2015", "DMY"), ///
								date("25/12/2015", "DMY"), ///
								date("29/10/2016", "DMY"), ///
								date("25/12/2016", "DMY"), ///
								date("26/12/2016", "DMY"))

	// gen school_holiday = inlist(date, td(9oct2015), td(7oct2016))
	// replace school_holiday = 1 if ym == ym(2015,11) & inlist(day, 21, 24, 25, 26, 29, 30)
	// replace school_holiday = 1 if ym == ym(2016,11) & inlist(day, 19, 22, 23, 24, 25, 16, 29, 30)
	
   	gen byte school_holiday = inlist(date, ///
   								date("9/10/2015", "DMY"), ///
   								date("21/11/2015", "DMY"), ///
   								date("24/11/2015", "DMY"), ///
   								date("25/11/2015", "DMY"), ///
   								date("26/11/2015", "DMY"), ///
   								date("27/11/2015", "DMY"), ///
   								date("28/11/2015", "DMY")) /* dec also holiday, controlled by month effect */
   	replace school_holiday = 1 if inlist(date, ///
   								date("7/10/2016", "DMY"), ///
   								date("19/11/2016", "DMY"), ///
   								date("22/11/2016", "DMY"), ///
   								date("23/11/2016", "DMY"), ///
	    						date("24/11/2016", "DMY"), ///
	    						date("25/11/2016", "DMY"), ///
	    						date("26/11/2016", "DMY"), ///
	    						date("29/11/2016", "DMY"), ///
	    						date("30/11/2016", "DMY")) /* dec also holiday, controlled by month effect */

	gen byte offday = inlist(dow, 0, 6) | holiday
	gen byte weekend_holiday = inlist(dow, 0, 6) & holiday
	gen byte christmas_period = month == 12 & inlist(day, 24, 25, 26, 27)
	gen byte chirstmas = month == 12 & day == 25
	
	gen byte oct2015 = date <= td(31oct2015)

	gen hot = temperature > 30

	cutandlabel rain, at(0 0.2 10 25 70) gen(rainlevel) label("Rain")
	cutandlabel pm25mean, at(0  15 30 40 50 150) gen(pm25level) label("PM 2.5")

	capture: encode card_type, gen(card_type_code)
	capture: encode travel_mode, gen(travel_mode_code)
end


capture: program drop label_vars
program label_vars
	capture: label var temperature Temperature
	capture: label var humidity Humidity
	capture: label var holiday Holiday
	capture: label var school_holiday "School holiday"
	capture: label var weekend_holiday "Weekend Holiday"

	capture: label var oct2015 "Oct 2015"
	capture: label define oct2015label 0 "after Oct-2015" 1 "Oct-2015"
	capture: label values oct2015 oct2015label

	capture: label var electricityLog "Electricity (log)"
	capture: label var christmas_period "Christmas (25-27 Dec)"


end

capture: program drop seperate_stop_by_mode
program define seperate_stop_by_mode
	syntax varname

	capture: drop mrt
	capture: drop mrt_station
	capture: drop mrt_stn

	gen byte mrt = strpos(`varlist', "STATION") > 0
	gen mrt_station = `varlist' if mrt
	encode mrt_station, gen(mrt_stn)

	replace `varlist' = "" if mrt | `varlist' == "null"
	destring `varlist', replace

	replace `varlist' = -999 if mrt
	replace mrt_stn = -999 if ~mrt

	drop mrt_station mrt
end
