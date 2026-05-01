@echo off
REM --- PICKLES AUCTION SCRAPER - FULL PIPELINE ---
title Pickles Auction Pipeline (Playwright)

echo Starting Pickles Auction Pipeline...
echo =======================================
echo Using Playwright (no ChromeDriver needed!)
echo.

REM ----------------------------------------
REM STEP 1: SCRAPE AUCTION SCHEDULE
REM ----------------------------------------
echo.
echo [1/7] Scraping auction schedule...
python scrapers\step1_scrape_schedule.py

echo Step 1 complete. Waiting 5 seconds...
timeout /t 5 /nobreak >nul

REM ----------------------------------------
REM STEP 2: GENERATE WATCH LINKS
REM ----------------------------------------
echo.
echo [2/7] Generating watch links...
python scrapers\step2_generate_link.py

echo Step 2 complete. Waiting 5 seconds...
timeout /t 5 /nobreak >nul

REM ----------------------------------------
REM STEP 3: GET USER EVENTS
REM ----------------------------------------
echo.
echo [3/7] Getting user events...
python scrapers\step3_get_user_event.py

echo Step 3 complete. Waiting 5 seconds...
timeout /t 5 /nobreak >nul

REM ----------------------------------------
REM STEP 4: JSON TO DATABASE
REM ----------------------------------------
echo.
echo [4/7] Loading JSON to database...
python scrapers\step4_json_to_db.py

echo Step 4 complete. Waiting 5 seconds...
timeout /t 5 /nobreak >nul

REM ----------------------------------------
REM STEP 5: TASK SCHEDULER FOR LISTING SCRAPER
REM ----------------------------------------
echo.
echo [5/7] Setting up listing scraper tasks...
python scrapers\listing_step2_task_scheduler.py

REM ----------------------------------------
REM STEP 6: TRACK VEHICLES BY STOCK NUMBER
REM ----------------------------------------
echo.
echo [6/7] Tracking vehicles...
python scrapers\listing_step5_track_vehicles.py

REM ----------------------------------------
REM STEP 7: VEHICLES TO DB + CLEANUP
REM ----------------------------------------
echo.
echo [7/7] Vehicles to DB + task cleanup...
python scrapers\listing_step6_vehicles_to_db.py
python scrapers\listing_step7_clean_task_scheduler.py

REM ----------------------------------------
:end
echo.
echo =======================================
echo Pipeline Finished!
pause