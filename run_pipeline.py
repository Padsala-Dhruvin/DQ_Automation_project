from dq_checks import run_all_dq_checks
import time

def main():
    start_time = time.time()
    print(f"\n--- Starting Scheduled DQ Pipeline at {time.ctime(start_time)} ---")
    pipeline_success = run_all_dq_checks()
    end_time = time.time()
    duration = round(end_time - start_time, 2)
    if pipeline_success:
        print(f"\n✨ PIPELINE RUN COMPLETE: Successful in {duration} seconds.")
    else:
        # In a real pipeline, an alert would be triggered here.
        print(f"\n❌ PIPELINE RUN COMPLETE: FAILED after {duration} seconds. Check logs.")
    print(f"\nData Quality Checks completed in {duration:.2f} seconds.")
if __name__ == '__main__':
    main()