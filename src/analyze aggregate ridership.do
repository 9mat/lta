set more off

set matsize 2000

do D:\Dropbox\work\script\LTA_common.do

global ezpath "H:\ezlink data"

global fg_path D:\Dropbox\work\script\fg\res_plot_aggregate_ridership
capture: mkdir "${fg_path}"

capture: program drop prepare_daily_rider_by_card_type
program define prepare_daily_rider_by_card_type
	local filelist: dir "${ezpath}\newalighting" files "*.dta", respectcase

	clear
	foreach filename in `filelist' {
		di "`filename'"
		append using "${ezpath}\newalighting\\`filename'"
	}

	keep if hour >= 8 & hour <= 20
	collapse (sum) tripDuration duration numOfRiders, by(card_type travel_mode date)
	save D:\Dropbox\work\data\ridersDailyByCardType.dta, replace
end

check_file_exists using D:\Dropbox\work\data\ridersDailyByCardType.dta, run(prepare_daily_rider_by_card_type)
merge_daily_weather_data



capture: program drop plot_env_ts
program define plot_env_ts
	use ${tmp}\daily_merge_env.dta, clear
	gen year = year(date)
	gen month = month(date)
	keep if inlist(year, 2015, 2016) & inlist(month, 10, 11, 12)
	gen doy = doy(date)
	xtset year doy

	tsline pm25mean, by(year, note("PM 2.5 is 8am-8pm hourly average") title("PM 2.5")) ytitle("PM 2.5") xtitle("day of year") scheme(sj) xline(`=doy(td(1nov2015))') xline(`=doy(td(1dec2015))')
	graph export ${fg_path}\ts_plot_pm25.pdf, replace

	tsline temperature, by(year, note("Temperature is 8am-8pm hourly average") title(Temperature))  ytitle("Temperature") xtitle("day of year") scheme(sj) xline(`=doy(td(1nov2015))') xline(`=doy(td(1dec2015))')
	graph export ${fg_path}\ts_plot_temperature.pdf, replace

	tsline humidity, by(year, note("Humidity is 8am-8pm hourly average")  title(Humidity)) ytitle("Humidity") xtitle("day of year") scheme(sj) xline(`=doy(td(1nov2015))') xline(`=doy(td(1dec2015))')
	graph export ${fg_path}\ts_plot_humidity.pdf, replace

	tsline rain, by(year, note("Rainfall is total rainfall 8am-8pm, average across 14 weather stations") title(Rainfall)) ytitle("Rainfall") xtitle("day of year") scheme(sj) xline(`=doy(td(1nov2015))') xline(`=doy(td(1dec2015))')
	graph export ${fg_path}\ts_plot_rain.pdf, replace
end





capture: program drop plot_rider_ts
program define plot_rider_ts
	use D:\Dropbox\work\data\ridersDailyByCardType.dta, clear
	separate numOfRiders, by(travel_mode)
	drop numOfRiders

	gen year = year(date)
	gen doy = doy(date)
	egen id = group(card_type year travel_mode)

	xtset id doy

	foreach ct in Adult Child Student "Senior Citizen" {
		tsline numOfRiders* if card_type=="`ct'", ///
			by(year,  title("`ct' daily ridership 8am-8pm")) ///
			xtitle(day of year) ytitle(ridership) legend(lab(1 Bus) lab(2 RTS)) scheme(sj)
		local ctstr = lower(subinstr("`ct'", " ", "_", .))
		graph export "${fg_path}\ts_plot_riders_`ctstr'.pdf", replace
	}
end


capture: program drop plot_res_riders_pm25
program define plot_res_riders_pm25

	use D:\Dropbox\work\data\ridersDailyByCardType.dta, clear
	merge m:1 date using ${tmp}\daily_merge_env.dta, keep(match master)
	gen_vars

	label define oct2015label 0 "after Oct-2015" 1 "Oct-2015"
	label values oct2015 oct2015label

	local covariates c.temperature c.humidity c.rainlevel* i.dow i.ym holiday c.weekend_holiday c.school_holiday c.christmas_period

	reg numOfRidersLog i.travel_mode_code##i.card_type_code##(`covariates')
	predict numOfRidersLogRes, res

	reg pm25mean `covariates'
	predict pm25meanRes, res

	label var numOfRidersLogRes "Ridership (log, residual)"
	label var pm25meanRes "PM2.5 (residual)"


	foreach ct in Adult Child Student "Senior Citizen" {
		twoway (scatter numOfRidersLogRes* pm25meanRes if card_type=="`ct'", msymbol(+ o)) ///
			(lfit numOfRidersLogRes pm25meanRes if card_type=="`ct'"), ///
			by(oct2015 travel_mode, cols(2) legend(off) ///
				note("Notes: Residuals after regressing on temperature, humidity, rain level, day of week, year-month," ///
					"holiday, weekend holiday, and their interactions with card type and travel mode" ///
					"obs = day*card-type*travel-mode, Ridership is daily 8am-8pm aggregate, pm2.5 is 8am-8pm average") ///
					title("`ct' ridership by mode of first segment vs PM 2.5")) ///
			ytitle("Ridership (log) residual") xtitle("PM2.5 residual") ///
			legend(off) scheme(sj) 

		local ctstr = lower(subinstr("`ct'", " ", "_", .))
		graph export "${fg_path}\res_plot_pm25_riders_`ctstr'.pdf", replace
	}
end

capture: program drop plot_res_riders_temperature
program define plot_res_riders_temperature
	use D:\Dropbox\work\data\ridersDailyByCardType.dta, clear
	merge m:1 date using ${tmp}\daily_merge_env.dta, keep(match master)
	gen_vars

	label define oct2015label 0 "after Oct-2015" 1 "Oct-2015"
	label values oct2015 oct2015label

	local covariates c.pm25level* c.rainlevel* i.dow i.ym holiday c.weekend_holiday c.school_holiday c.christmas_period
	reg numOfRidersLog i.travel_mode_code##i.card_type_code##(`covariates')
	predict numOfRidersLogRes, res

	reg temperature `covariates'
	predict temperatureRes, res

	foreach ct in Adult Child Student "Senior Citizen" {
		twoway (scatter numOfRidersLogRes temperatureRes if card_type=="`ct'", msymbol(+ o)) ///
			(lfit numOfRidersLogRes temperatureRes if card_type=="`ct'"), ///
			by(travel_mode, cols(2) legend(off) ///
				note("Notes: Residuals after regressing on humidity, rain level, day of week, year-month," ///
					"holiday, weekend holiday, and their interactions with card type and travel mode" ///
					"obs = day*card-type*travel-mode, Ridership is daily 8am-8pm aggregate, pm2.5 is 8am-8pm average") ///
					title("`ct' ridership by mode of first segment vs Temperature")) ///
			ytitle("Ridership (log) residual") xtitle("Temperature residual") ///
			legend(off) scheme(sj) 

		local ctstr = lower(subinstr("`ct'", " ", "_", .))
		graph export "${fg_path}\res_plot_temperature_riders_`ctstr'.pdf", replace
	}

end


capture: program drop plot_res_riders_pm25_bothmode
program define plot_res_riders_pm25_bothmode

	use D:\Dropbox\work\data\ridersDailyByCardType.dta, clear
	collapse (sum) numOfRiders, by(date card_type)
	merge m:1 date using ${tmp}\daily_merge_env.dta, keep(match master)

	gen_vars
	keep if inlist(year, 2015, 2016) & inlist(month, 10, 11, 12)

	label define oct2015label 0 "after Oct-2015" 1 "Oct-2015"
	label values oct2015 oct2015label

	local covariates c.temperature c.humidity c.rainlevel* i.dow i.ym holiday c.weekend_holiday c.school_holiday c.christmas_period

	reg numOfRidersLog i.card_type_code##(`covariates')
	predict numOfRidersLogRes, res

	reg pm25mean `covariates'
	predict pm25meanRes, res


	foreach ct in Adult Child Student "Senior Citizen" {
		twoway (scatter numOfRidersLogRes pm25meanRes if card_type=="`ct'", msymbol(+ o)) ///
			(lfit numOfRidersLogRes pm25meanRes if card_type=="`ct'"), ///
			by(oct2015, cols(2) legend(off) ///
				note("Notes: Residuals after regressing on temperature, humidity, rain level, day of week, year-month," ///
					"holiday, weekend holiday, and their interactions with card type and travel mode" ///
					"obs = day*card-type*travel-mode, Ridership is daily 8am-8pm aggregate, pm2.5 is 8am-8pm average") ///
					title("`ct' ridership vs PM 2.5")) ///
			ytitle("Ridership (log) residual") xtitle("PM2.5 residual") ///
			legend(off) scheme(sj) 

		local ctstr = lower(subinstr("`ct'", " ", "_", .))
		graph export "${fg_path}\res_plot_pm25_both_mode_riders_`ctstr'.pdf", replace
	}

end




// plot_env_ts
// plot_rider_ts
// plot_res_riders_pm25
// plot_res_riders_temperature
// plot_res_riders_pm25_bothmode





capture: program drop plot_res_trips_pm25
program define plot_res_trips_pm25

	import delimited D:\Dropbox\work\data\hourly_trip_by_ct_tm.csv, clear 
	rename date datestr
	gen date = date(datestr, "YMD")
	collapse (sum) numOfRiders = numoftrips if hour >= 8 & hour <= 20, by(date card_type travel_mode)

	merge m:1 date using ${tmp}\daily_merge_env.dta, keep(match master)
	gen_vars

	label define oct2015label 0 "after Oct-2015" 1 "Oct-2015"
	label values oct2015 oct2015label

	local covariates c.temperature c.humidity c.rainlevel* i.dow i.ym holiday c.weekend_holiday c.school_holiday c.christmas_period

	reg numOfRidersLog i.travel_mode_code##i.card_type_code##(`covariates')
	predict numOfRidersLogRes, res

	reg pm25mean `covariates'
	predict pm25meanRes, res

	label var numOfRidersLogRes "Ridership (log, residual)"
	label var pm25meanRes "PM2.5 (residual)"


	foreach ct in Adult Child Student "Senior Citizen" {
		twoway (scatter numOfRidersLogRes* pm25meanRes if card_type=="`ct'", msymbol(+ o)) ///
			(lfit numOfRidersLogRes pm25meanRes if card_type=="`ct'"), ///
			by(oct2015 travel_mode, cols(2) legend(off) ///
				note("Notes: Residuals after regressing on temperature, humidity, rain level, day of week, year-month," ///
					"holiday, weekend holiday, and their interactions with card type and travel mode" ///
					"obs = day*card-type*travel-mode, Ridership is daily 8am-8pm aggregate, pm2.5 is 8am-8pm average") ///
					title("Number of trips by `ct' vs PM 2.5")) ///
			ytitle("Number of trips (log) residual") xtitle("PM2.5 residual") ///
			legend(off) scheme(sj) 

		local ctstr = lower(subinstr("`ct'", " ", "_", .))
		graph export "${fg_path}\res_plot_pm25_trip_`ctstr'.pdf", replace
	}
end


capture: program drop plot_res_trips_temperature
program define plot_res_trips_temperature
	import delimited D:\Dropbox\work\data\hourly_trip_by_ct_tm.csv, clear 
	rename date datestr
	gen date = date(datestr, "YMD")
	collapse (sum) numOfRiders = numoftrips if hour >= 8 & hour <= 20, by(date card_type travel_mode)

	merge m:1 date using ${tmp}\daily_merge_env.dta, keep(match master)
	gen_vars

	label define oct2015label 0 "after Oct-2015" 1 "Oct-2015"
	label values oct2015 oct2015label

	local covariates c.pm25level* c.rainlevel* i.dow i.ym holiday c.weekend_holiday c.school_holiday c.christmas_period
	reg numOfRidersLog i.travel_mode_code##i.card_type_code##(`covariates')
	predict numOfRidersLogRes, res

	reg temperature `covariates'
	predict temperatureRes, res

	foreach ct in Adult Child Student "Senior Citizen" {
		twoway (scatter numOfRidersLogRes temperatureRes if card_type=="`ct'", msymbol(+ o)) ///
			(lfit numOfRidersLogRes temperatureRes if card_type=="`ct'"), ///
			by(travel_mode, cols(2) legend(off) ///
				note("Notes: Residuals after regressing on humidity, rain level, day of week, year-month," ///
					"holiday, weekend holiday, and their interactions with card type and travel mode" ///
					"obs = day*card-type*travel-mode, Ridership is daily 8am-8pm aggregate, pm2.5 is 8am-8pm average") ///
					title("Number of `ct' trips vs Temperature")) ///
			ytitle("Number of trips (log) residual") xtitle("Temperature residual") ///
			legend(off) scheme(sj) 

		local ctstr = lower(subinstr("`ct'", " ", "_", .))
		graph export "${fg_path}\res_plot_temperature_trips_`ctstr'.pdf", replace
	}

end


// plot_res_trips_pm25
// plot_res_trips_temperature


capture: program drop plot_aggregate_res_trips_env
program define plot_aggregate_res_trips_env
	use D:\Dropbox\work\data\ridersDailyByCardType.dta, clear
	collapse (sum) numOfRiders, by(date)

	merge m:1 date using ${tmp}\daily_merge_env.dta, keep(match master)
	gen_vars
	drop if date == td(26dec2015)
	drop if date == td(26oct2015)

	label define oct2015label 0 "after Oct-2015" 1 "Oct-2015"
	label values oct2015 oct2015label

	local covariates i.dow i.ym holiday c.weekend_holiday c.school_holiday c.christmas_period

	capture: drop *Reg

	foreach x in numOfRidersLog temperature pm25mean humidity rain {
		reg `x' `covariates'
		predict `x'Res, res		
	}

	foreach x in temperature pm25mean humidity rain {
		twoway (scatter numOfRidersLogRes `x'Res) ///
			(lfit numOfRidersLogRes `x'Res), ///
			by(oct2015, note("Notes: Residuals after regressing on day of week, year-month," ///
				"holiday, weekend holiday, school holiday,and Christmas period") ///
			title("Number of trips vs `x', partial out calendar effects") cols(2)) ///
			ytitle("Number of trips (log) residual") ///
			xtitle("`x' residual") ///
			legend(off) scheme(sj) 

		graph export "${fg_path}\res_plot_`x'_aggtrips.pdf", replace
	}

	capture: drop *Reg


	foreach x in temperature pm25mean humidity humidity rain {
		local covariates i.dow i.ym holiday c.weekend_holiday c.school_holiday c.christmas_period
		local xlist ""
		foreach x2 in  temperature pm25mean humidity rain {
			if "`x'" ~= "`x2'" {
				local covariates `x2' `covariates'
				local xlist "`x2', `xlist'"
			}
		}

		di "`covariates'"
		capture: drop *Res

		reg numOfRidersLog `covariates'
		predict numOfRidersLogRes, res

		reg `x' `covariates'
		predict `x'Res, res		

		twoway (scatter numOfRidersLogRes `x'Res) ///
			(lfit numOfRidersLogRes `x'Res), ///
			by(oct2015, note("Notes: Residuals after regressing on `xlist'day ofweek, year-month," ///
				"holiday, weekend holiday, school holiday, and Christmas period") ///
			title("Number of trips vs `x', full controls") cols(2)) ///
			ytitle("Number of trips (log) residual") ///
			xtitle("`x' residual") ///
			legend(off) scheme(sj) 
		graph export "${fg_path}\res_plot_`x'_aggtrips_fullcontrols.pdf", replace
	}


end




capture: program drop plot_aggregate_res_rides_env
program define plot_aggregate_res_rides_env
	use D:\Dropbox\work\data\ridersDailyByCardType.dta, clear
	collapse (sum) numOfRiders, by(date)

	merge m:1 date using ${tmp}\daily_merge_env.dta, keep(match master)
	gen_vars

	label define oct2015label 0 "after Oct-2015" 1 "Oct-2015"
	label values oct2015 oct2015label

	local covariates i.dow i.ym holiday c.weekend_holiday c.school_holiday c.christmas_period

	capture: drop *Reg

	foreach x in numOfRidersLog temperature pm25mean humidity rain {
		reg `x' `covariates'
		predict `x'Res, res		
	}

	foreach x in temperature pm25mean humidity rain {
		twoway (scatter numOfRidersLogRes `x'Res) ///
			(lfit numOfRidersLogRes `x'Res), ///
			by(oct2015, note("Notes: Residuals after regressing on day of week, year-month," ///
				"holiday, weekend holiday, school holiday,and Christmas period") ///
			title("Number of riders vs `x', partial out calendar effects") cols(2)) ///
			ytitle("Number of riders (log) residual") ///
			xtitle("`x' residual") ///
			legend(off) scheme(sj) 

		graph export "${fg_path}\res_plot_`x'_aggrides.pdf", replace
	}

	capture: drop *Reg


	foreach x in temperature pm25mean humidity rain {
		local covariates i.dow i.ym holiday c.weekend_holiday c.school_holiday c.christmas_period
		local xlist ""
		foreach x2 in  temperature pm25mean humidity rain {
			if "`x'" ~= "`x2'" {
				local covariates `x2' `covariates'
				local xlist "`x2', `xlist'"
			}
		}

		di "`covariates'"
		capture: drop *Res

		reg numOfRidersLog `covariates'
		predict numOfRidersLogRes, res

		reg `x' `covariates'
		predict `x'Res, res		

		twoway (scatter numOfRidersLogRes `x'Res) ///
			(lfit numOfRidersLogRes `x'Res), ///
			by(oct2015, note("Notes: Residuals after regressing on `xlist'day ofweek, year-month," ///
				"holiday, weekend holiday, school holiday, and Christmas period") ///
			title("Number of riders vs `x', full controls") cols(2)) ///
			ytitle("Number of riders (log) residual") ///
			xtitle("`x' residual") ///
			legend(off) scheme(sj) 
		graph export "${fg_path}\res_plot_`x'_aggrides_fullcontrols.pdf", replace
	}


end

// plot_aggregate_res_trips_env
// plot_aggregate_res_rides_env

local res_note `""the regression for the residuals includes PM 2.5, temperature, humidity, rain level," "day of week, year*month, holiday, weekend holiday and school holiday""'
capture: program drop plot_res_riders_pm25_avplot

global fg2path "D:\Dropbox\work\script\fg\res_plot_aggregate_ridership_v2"
capture: mkdir ${fg2path}
program define plot_res_riders_pm25_avplot

	* total ridership
	use D:\Dropbox\work\data\ridersDailyByCardType.dta, clear
	collapse (sum) numOfRiders, by(date)
	merge m:1 date using ${tmp}\daily_merge_env.dta, keep(match master)
	gen_vars
	label_vars

	local indepvars pm25mean temperature humidity rainlevel* holiday weekend_holiday school_holiday i.dow i.year i.month

	reg numOfRidersLog `indepvars'
	avplot pm25mean, title("Residual plot: daily ridership vs. PM 2.5 (obs=day)")
	graph export ${fg2path}/avplot_rider_pm25_agg.pdf, replace

	avplot temperature, title("Residual plot: daily ridership vs. Temperature (obs=day)")
	graph export ${fg2path}/avplot_rider_temperature_agg.pdf, replace
	
	reg numOfRidersLog `indepvars' if ~oct2015, r
	avplot pm25mean, title("Residual plot: daily ridership vs. PM 2.5 (obs=day)") subtitle("excluding Oct 2015")
	graph export ${fg2path}/avplot_rider_pm25_agg_exl201510.pdf, replace

	reg numOfRidersLog `indepvars' if ~offday, r
	avplot pm25mean, title("Residual plot: daily ridership vs. PM 2.5 (obs=day)") subtitle("Work days")
	graph export ${fg2path}/avplot_rider_pm25_agg_workdays.pdf, replace
	avplot temperature, title("Residual plot: daily ridership vs. Temperature(obs=day)") subtitle("Work days")
	graph export ${fg2path}/avplot_rider_temperature_agg_workdays.pdf, replace

	reg numOfRidersLog `indepvars' if offday, r
	avplot pm25mean, title("Residual plot: daily ridership vs. PM 2.5 (obs=day)") subtitle("Off days")
	graph export ${fg2path}/avplot_rider_pm25_agg_offdays.pdf, replace
	avplot temperature, title("Residual plot: daily ridership vs. Temperature (obs=day)") subtitle("Off days")
	graph export ${fg2path}/avplot_rider_temperature_agg_offdays.pdf, replace

	use D:\Dropbox\work\data\ridersDailyByCardType.dta, clear
	collapse (sum) numOfRiders, by(date card_type)
	merge m:1 date using ${tmp}\daily_merge_env.dta, keep(match master)
	gen_vars
	label_vars

	foreach ct in Adult Child Student "Senior Citizen" {
		local ctstr = lower(subinstr("`ct'", " ", "_", .))

		reg numOfRidersLog `indepvars' if card_type=="`ct'", r
		avplot pm25mean, title("Residual plot: daily ridership vs. PM 2.5 (obs=day)") subtitle(`ct')
		graph export ${fg2path}/avplot_rider_pm25_`ctstr'.pdf, replace

		avplot temperature, title("Residual plot: daily ridership vs. Temperature (obs=day)") subtitle(`ct')
		graph export ${fg2path}/avplot_rider_temperature_`ctstr'.pdf, replace
		
		reg numOfRidersLog `indepvars' if card_type=="`ct'" & ~oct2015, r
		avplot pm25mean, title("Residual plot: daily ridership vs. PM 2.5 (obs=day)") subtitle("`ct', exlcuding Oct 2015")
		graph export ${fg2path}/avplot_rider_pm25_`ctstr'_exl201510.pdf, replace
	}
end

plot_res_riders_pm25_avplot
