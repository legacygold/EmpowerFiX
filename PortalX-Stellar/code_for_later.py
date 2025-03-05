if __name__ == "__main__":
    conn = get_db_connection
    archive_old_data(conn)  # Archive old data older than 30 days old

    # Start the initial setup and data extraction to get up to date historical trade data
    initial_setup_and_data_extraction()

    # Separate process for continuous data streaming and updating
    data_stream_process = multiprocessing.Process(target=start_streams, args=(conn,))
    data_stream_process.start()

    # Separate process for continuous candle data generation and updating
    candle_update_process = multiprocessing.Process(target=generate_and_update_candle_data_loop, args=(conn, definitions,))
    candle_update_process.start()

    # User interaction for starting inital cycle set when first starting the program
    start_cycle_set_interaction()

    # User interaction process for starting new cycle sets
    user_interaction_new_cycleset_process = multiprocessing.Process(target=prompt_user_to_start_new_cycleset)
    user_interaction_new_cycleset_process.start()

    # Here, manage the processes as needed, possibly waiting for some to complete or allowing them to run indefinitely


