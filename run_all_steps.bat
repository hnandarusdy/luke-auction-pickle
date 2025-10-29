@echo off
REM --- SETTING UP THE WORKFLOW SCRIPT ---
title Python Workflow Runner (Timed Delay)

echo Starting Python workflow...
echo =======================================

REM ----------------------------------------
REM STEP 1: SCRAPE AUCTIONS
REM ----------------------------------------
echo.
echo [1/4] Running step1_scrape_auctions.py
python  step1_scrape_pickle_schedule.py

REM --- TIMED DELAY (5 seconds) ---
REM Waits for 5 seconds automatically before moving to the next step.
echo.
echo Step 1 complete. Waiting 5 seconds before proceeding to Step 2...
timeout /t 5 /nobreak >nul

REM ----------------------------------------
REM STEP 2: GENERATE LINK
REM ----------------------------------------
echo.
echo [2/4] Running step2_generate_link.py
python step2_generate_link.py

REM --- TIMED DELAY (5 seconds) ---
echo.
echo Step 2 complete. Waiting 5 seconds before proceeding to Step 3...
timeout /t 5 /nobreak >nul

REM ----------------------------------------
REM STEP 3: GET USER EVENT
REM ----------------------------------------
echo.
echo [3/4] Running step3_get_user_event.py
python step3_get_user_event.py

REM --- TIMED DELAY (5 seconds) ---
echo.
echo Step 3 complete. Waiting 5 seconds before proceeding to the final step...
timeout /t 5 /nobreak >nul

REM ----------------------------------------
REM STEP 4: JSON TO DB
REM ----------------------------------------
echo.
echo [4/4] Running step4_json_to_db.py
python step4_json_to_db.py
echo.

python listing_scraper_step5_track_vehicles_by_stockno.py
python listing_scraper_step6_vehicles_to_db.py

REM ----------------------------------------
:end
echo =======================================
echo Workflow Finished.
pause