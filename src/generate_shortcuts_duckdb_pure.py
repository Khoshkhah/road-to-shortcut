
import logging
from pathlib import Path
import duckdb
import config
import utilities_duckdb as utils

import logging_config_duckdb as log_conf

# Setup logging
# Note: Handlers are configured by setup_logging when called from main.
logger = logging.getLogger(__name__)

def compute_shortest_paths_pure_duckdb(con: duckdb.DuckDBPyConnection, max_iterations: int = 100):
    """
    Compute shortest paths using iterative SQL (Bellman-Ford-ish / Delta Stepping).
    Input: 'shortcuts_active' table (subset of shortcuts valid for current cell).
    Output: 'shortcuts_next' table with all-pairs shortest paths.
    """
    logger.info("Starting pure DuckDB shortest path computation")
    
    # 1. Initialize 'paths' with base shortcuts
    con.execute("DROP TABLE IF EXISTS paths")
    con.execute("""
        CREATE TABLE paths AS 
        SELECT from_edge, to_edge, cost, via_edge, current_cell 
        FROM shortcuts_active
    """)
    
    stats = con.sql("SELECT COUNT(*), SUM(cost) FROM paths").fetchone()
    logger.info(f"Initial: {stats[0]} paths, CostSum: {stats[1] if stats[1] else 0.0:.4f}")
    
    # 2. Iterative expansion
    i = 0
    while i < max_iterations:
        stats_before = con.sql("SELECT COUNT(*), SUM(cost) FROM paths").fetchone()
        row_count_before = stats_before[0]
        cost_sum_before = stats_before[1] if stats_before[1] is not None else 0.0
        
        # Geometric expansion: paths JOIN paths
        con.execute("""
            CREATE OR REPLACE TABLE new_paths AS
            SELECT 
                L.from_edge,
                R.to_edge,
                L.cost + R.cost AS cost,
                L.to_edge AS via_edge,
                L.current_cell
            FROM paths L
            JOIN paths R ON L.to_edge = R.from_edge AND L.current_cell = R.current_cell
            WHERE L.from_edge != R.to_edge
        """)
        
        # Merge new paths into existing paths, keeping MIN cost
        con.execute("""
            CREATE OR REPLACE TABLE combined_paths AS
            SELECT * FROM paths
            UNION ALL
            SELECT * FROM new_paths
        """)
        
        con.execute("""
            CREATE OR REPLACE TABLE paths_reduced AS
            SELECT 
                from_edge, 
                to_edge, 
                min(cost) as cost,
                first(via_edge) as via_edge,
                current_cell
            FROM combined_paths
            GROUP BY from_edge, to_edge, current_cell
        """)
        
        con.execute("DROP TABLE paths")
        con.execute("ALTER TABLE paths_reduced RENAME TO paths")
        
        # Convergence check
        stats = con.sql("SELECT COUNT(*), SUM(cost) FROM paths").fetchone()
        row_count_after = stats[0]
        cost_sum_after = stats[1] if stats[1] is not None else 0.0
        
        cost_diff = cost_sum_before - cost_sum_after
        logger.info(f"Iteration {i}: Rows {row_count_before} -> {row_count_after}, CostSum {cost_sum_before:.4f} -> {cost_sum_after:.4f} (diff: {cost_diff:.4f})")
        
        # Stop if STABLE (no new rows AND no cost improvement)
        if row_count_after == row_count_before and abs(cost_sum_after - cost_sum_before) < 1e-6:
            logger.info("Converged.")
            break
            
        i += 1
        
    con.execute("DROP TABLE IF EXISTS shortcuts_next")
    con.execute("ALTER TABLE paths RENAME TO shortcuts_next")


def main():
    log_conf.setup_logging("generate_shortcuts_duckdb_pure")
    log_conf.log_section(logger, "SHORTCUTS GENERATION - PURE DUCKDB VERSION")
    
    config_info = {
        "edges_file": str(config.EDGES_FILE),
        "graph_file": str(config.GRAPH_FILE),
        "output_file": str(config.SHORTCUTS_OUTPUT_FILE),
        "district": config.DISTRICT_NAME
    }
    log_conf.log_dict(logger, config_info, "Configuration")
    
    # Define unique database path if persistence is enabled
    db_path = ":memory:"
    if config.DUCKDB_PERSIST_DIR:
        db_path = str(Path(config.DUCKDB_PERSIST_DIR) / "pure_working.db")
        logger.info(f"Using file-backed DuckDB: {db_path}")
        
    con = utils.initialize_duckdb(db_path)
    
    # 1. Load Data
    logger.info("Loading edge data...")
    utils.read_edges(con, str(config.EDGES_FILE))
    edges_count = con.sql("SELECT COUNT(*) FROM edges").fetchone()[0]
    logger.info(f"✓ Loaded {edges_count} edges")

    logger.info("Computing edge costs...")
    utils.create_edges_cost_table(con, str(config.EDGES_FILE))
    logger.info("✓ Edge costs computed")
    
    logger.info("Creating initial shortcuts table...")
    utils.initial_shortcuts_table(con, str(config.GRAPH_FILE))
    shortcuts_count = con.sql("SELECT COUNT(*) FROM shortcuts").fetchone()[0]
    logger.info(f"✓ Created {shortcuts_count} initial shortcuts")
    
    resolution_results = []

    # 2. Forward Pass (15 → -1)
    log_conf.log_section(logger, "PHASE 1: FORWARD PASS (15 → -1)")
    for res in range(15, -2, -1):  # 15, 14, ..., 0, -1
        logger.info(f"\nForward: Resolution {res}")
        
        # A. Assign Cells
        logger.info(f"Assigning cells for resolution {res}...")
        utils.assign_cell_forward(con, res)
        con.execute("DROP TABLE IF EXISTS shortcuts_active")
        con.execute("ALTER TABLE shortcuts_next RENAME TO shortcuts_active")
        
        # B. Filter active shortcuts (current_cell IS NOT NULL)
        con.execute("DELETE FROM shortcuts_active WHERE current_cell IS NULL")
        
        # C. Run Algorithm
        active_count = con.sql("SELECT COUNT(*) FROM shortcuts_active").fetchone()[0]
        logger.info(f"✓ {active_count} active shortcuts at resolution {res}")
        
        new_count = 0
        if active_count > 0:
            compute_shortest_paths_pure_duckdb(con)
            new_count = con.sql("SELECT COUNT(*) FROM shortcuts_next").fetchone()[0]
            logger.info(f"✓ Generated {new_count} shortcuts")
        else:
            logger.info("No active shortcuts, skipping...")
            con.execute("CREATE OR REPLACE TABLE shortcuts_next AS SELECT * FROM shortcuts_active WHERE 1=0")
        
        resolution_results.append({
            "phase": "forward",
            "resolution": res,
            "active": active_count,
            "generated": new_count
        })

        # D. Merge
        logger.info(f"Merging {new_count} new shortcuts...")
        utils.merge_shortcuts(con)
        utils.checkpoint(con)
        
        # Cleanup
        con.execute("DROP TABLE IF EXISTS shortcuts_active")

    # 3. Backward Pass (0 → 15)
    log_conf.log_section(logger, "PHASE 2: BACKWARD PASS (0 → 15)")
    for res in range(0, 16):  # 0, 1, ..., 15
        logger.info(f"\nBackward: Resolution {res}")
        
        # A. Assign Cells (backward)
        logger.info(f"Assigning cells for resolution {res}...")
        utils.assign_cell_backward(con, res)
        con.execute("DROP TABLE IF EXISTS shortcuts_active")
        con.execute("ALTER TABLE shortcuts_next RENAME TO shortcuts_active")
        
        # B. Filter active shortcuts (current_cell IS NOT NULL)
        con.execute("DELETE FROM shortcuts_active WHERE current_cell IS NULL")
        
        # C. Run Algorithm
        active_count = con.sql("SELECT COUNT(*) FROM shortcuts_active").fetchone()[0]
        logger.info(f"✓ {active_count} active shortcuts at resolution {res}")
        
        new_count = 0
        if active_count > 0:
            compute_shortest_paths_pure_duckdb(con)
            new_count = con.sql("SELECT COUNT(*) FROM shortcuts_next").fetchone()[0]
            logger.info(f"✓ Generated {new_count} shortcuts")
        else:
            logger.info("No active shortcuts, skipping...")
            con.execute("CREATE OR REPLACE TABLE shortcuts_next AS SELECT * FROM shortcuts_active WHERE 1=0")
        
        resolution_results.append({
            "phase": "backward",
            "resolution": res,
            "active": active_count,
            "generated": new_count
        })

        # D. Merge
        logger.info(f"Merging {new_count} new shortcuts...")
        utils.merge_shortcuts(con)
        utils.checkpoint(con)
        
        # Cleanup
        con.execute("DROP TABLE IF EXISTS shortcuts_active")

    # 4. Finalize
    log_conf.log_section(logger, "SAVING OUTPUT")
    
    final_count = con.sql("SELECT COUNT(*) FROM shortcuts").fetchone()[0]
    logger.info(f"Final shortcuts count: {final_count}")

    logger.info("Adding final info (cell, inside)...")
    utils.add_final_info(con)
    
    output_path = str(config.SHORTCUTS_OUTPUT_FILE).replace("_shortcuts", "_duckdb_pure")
    logger.info(f"Saving to {output_path}")
    utils.save_output(con, output_path)
    
    # Summary
    log_conf.log_section(logger, "SUMMARY")
    for r in resolution_results:
        logger.info(f"  {r['phase']:8s} res={r['resolution']:2d}: {r['active']} active → {r['generated']} generated")
    logger.info(f"\n✓ Total shortcuts: {final_count}")
    
    log_conf.log_section(logger, "COMPLETED")
    logger.info("Done.")

if __name__ == "__main__":
    main()
