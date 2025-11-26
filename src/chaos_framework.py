#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Chaos Engineering Framework for Databases
Tests system resilience through controlled failure injection
"""

import psycopg2
import time
import random
from datetime import datetime
from typing import Dict, List
import logging

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


class ChaosExperiment:
    """Represents a chaos experiment"""
    
    def __init__(self, name: str, description: str, blast_radius: str):
        self.name = name
        self.description = description
        self.blast_radius = blast_radius
        self.start_time = None
        self.end_time = None
        self.result = None
        self.observations = []


class ChaosFramework:
    """Chaos Engineering Framework for Database Testing"""
    
    def __init__(self):
        self.conn = None
        self.experiments = []
        self.baseline_metrics = {}
        
    def connect(self):
        try:
            self.conn = psycopg2.connect(
                host='localhost', port=5460,
                dbname='chaos_db', user='postgres', password='postgres'
            )
            self.conn.autocommit = True
            logger.info("Connected to target database")
            return True
        except Exception as e:
            logger.error(f"Connection failed: {e}")
            return False
    
    def setup(self):
        """Setup test database"""
        cursor = self.conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS test_data (
                id SERIAL PRIMARY KEY,
                data TEXT,
                created_at TIMESTAMP DEFAULT NOW()
            );
            
            INSERT INTO test_data (data)
            SELECT 'Test data ' || i
            FROM generate_series(1, 1000) i
            ON CONFLICT DO NOTHING;
        """)
        cursor.close()
        logger.info("Test database ready")
    
    def capture_baseline(self):
        """Capture baseline metrics before chaos"""
        
        logger.info("Capturing baseline metrics...")
        
        cursor = self.conn.cursor()
        
        # Query latency
        start = time.time()
        cursor.execute("SELECT COUNT(*) FROM test_data")
        cursor.fetchone()
        latency = (time.time() - start) * 1000
        
        # Connection count
        cursor.execute("SELECT count(*) FROM pg_stat_activity")
        connections = cursor.fetchone()[0]
        
        cursor.close()
        
        self.baseline_metrics = {
            'query_latency_ms': latency,
            'active_connections': connections,
            'timestamp': datetime.now()
        }
        
        logger.info(f"  Baseline latency: {latency:.2f}ms")
        logger.info(f"  Baseline connections: {connections}")
    
    def inject_high_cpu_load(self, duration: int = 5):
        """Inject high CPU load"""
        
        experiment = ChaosExperiment(
            name="High CPU Load",
            description="Simulate CPU saturation with expensive queries",
            blast_radius="database"
        )
        
        experiment.start_time = datetime.now()
        
        logger.info("Injecting CPU load...")
        
        cursor = self.conn.cursor()
        
        # Run expensive query
        try:
            cursor.execute("""
                SELECT COUNT(*) FROM test_data t1 
                CROSS JOIN test_data t2 
                LIMIT 100000
            """)
            cursor.fetchall()
            
            experiment.observations.append("CPU-intensive query executed")
            experiment.result = "completed"
            
        except Exception as e:
            experiment.observations.append(f"Error: {e}")
            experiment.result = "failed"
        
        cursor.close()
        
        experiment.end_time = datetime.now()
        self.experiments.append(experiment)
        
        logger.info("  CPU load experiment complete")
        
        return experiment
    
    def inject_connection_saturation(self):
        """Saturate connection pool"""
        
        experiment = ChaosExperiment(
            name="Connection Saturation",
            description="Exhaust available database connections",
            blast_radius="connection_pool"
        )
        
        experiment.start_time = datetime.now()
        
        logger.info("Saturating connection pool...")
        
        connections = []
        max_connections = 10
        
        try:
            for i in range(max_connections):
                conn = psycopg2.connect(
                    host='localhost', port=5460,
                    dbname='chaos_db', user='postgres', password='postgres'
                )
                connections.append(conn)
            
            experiment.observations.append(f"Created {len(connections)} connections")
            
            # Hold connections briefly
            time.sleep(2)
            
            # Test if new connections fail
            try:
                extra_conn = psycopg2.connect(
                    host='localhost', port=5460,
                    dbname='chaos_db', user='postgres', password='postgres',
                    connect_timeout=2
                )
                extra_conn.close()
                experiment.observations.append("Additional connection succeeded")
            except:
                experiment.observations.append("Additional connection blocked (expected)")
            
            experiment.result = "completed"
            
        except Exception as e:
            experiment.observations.append(f"Error: {e}")
            experiment.result = "failed"
        
        finally:
            # Cleanup
            for conn in connections:
                conn.close()
        
        experiment.end_time = datetime.now()
        self.experiments.append(experiment)
        
        logger.info("  Connection saturation experiment complete")
        
        return experiment
    
    def inject_slow_queries(self):
        """Inject artificially slow queries"""
        
        experiment = ChaosExperiment(
            name="Slow Query Injection",
            description="Simulate database performance degradation",
            blast_radius="query_performance"
        )
        
        experiment.start_time = datetime.now()
        
        logger.info("Injecting slow queries...")
        
        cursor = self.conn.cursor()
        
        try:
            # Simulate slow query with pg_sleep
            cursor.execute("SELECT pg_sleep(2)")
            cursor.fetchone()
            
            # Measure impact on subsequent queries
            start = time.time()
            cursor.execute("SELECT COUNT(*) FROM test_data")
            cursor.fetchone()
            latency = (time.time() - start) * 1000
            
            experiment.observations.append(f"Query latency during chaos: {latency:.2f}ms")
            experiment.observations.append(f"Baseline latency: {self.baseline_metrics['query_latency_ms']:.2f}ms")
            
            degradation = ((latency - self.baseline_metrics['query_latency_ms']) / 
                          self.baseline_metrics['query_latency_ms'] * 100)
            
            experiment.observations.append(f"Performance degradation: {degradation:.1f}%")
            experiment.result = "completed"
            
        except Exception as e:
            experiment.observations.append(f"Error: {e}")
            experiment.result = "failed"
        
        cursor.close()
        
        experiment.end_time = datetime.now()
        self.experiments.append(experiment)
        
        logger.info("  Slow query experiment complete")
        
        return experiment
    
    def inject_random_failures(self):
        """Randomly fail transactions"""
        
        experiment = ChaosExperiment(
            name="Random Transaction Failures",
            description="Test application error handling",
            blast_radius="transactions"
        )
        
        experiment.start_time = datetime.now()
        
        logger.info("Injecting random failures...")
        
        cursor = self.conn.cursor()
        
        success_count = 0
        failure_count = 0
        
        for i in range(10):
            try:
                # Randomly fail some operations
                if random.random() < 0.3:  # 30% failure rate
                    cursor.execute("SELECT 1/0")  # Intentional error
                    failure_count += 1
                else:
                    cursor.execute("SELECT 1")
                    cursor.fetchone()
                    success_count += 1
                    
            except Exception as e:
                failure_count += 1
        
        experiment.observations.append(f"Success: {success_count}, Failures: {failure_count}")
        experiment.result = "completed"
        
        cursor.close()
        
        experiment.end_time = datetime.now()
        self.experiments.append(experiment)
        
        logger.info(f"  Random failures experiment complete")
        
        return experiment
    
    def test_recovery_time(self):
        """Test recovery after chaos"""
        
        logger.info("Testing recovery time...")
        
        cursor = self.conn.cursor()
        
        recovery_start = time.time()
        attempts = 0
        max_attempts = 10
        
        while attempts < max_attempts:
            try:
                cursor.execute("SELECT 1")
                cursor.fetchone()
                break
            except:
                attempts += 1
                time.sleep(0.5)
        
        recovery_time = (time.time() - recovery_start) * 1000
        
        cursor.close()
        
        logger.info(f"  Recovery time: {recovery_time:.2f}ms ({attempts} attempts)")
        
        return recovery_time
    
    def generate_report(self):
        """Generate chaos experiment report"""
        
        print("\n" + "=" * 80)
        print("CHAOS ENGINEERING EXPERIMENT REPORT")
        print("=" * 80)
        print(f"Total Experiments: {len(self.experiments)}")
        
        completed = [e for e in self.experiments if e.result == 'completed']
        failed = [e for e in self.experiments if e.result == 'failed']
        
        print(f"Completed: {len(completed)}")
        print(f"Failed: {len(failed)}")
        
        print("\n" + "=" * 80)
        print("EXPERIMENT DETAILS")
        print("=" * 80)
        
        for i, exp in enumerate(self.experiments, 1):
            duration = (exp.end_time - exp.start_time).total_seconds()
            
            print(f"\n[{i}] {exp.name}")
            print(f"    Description: {exp.description}")
            print(f"    Blast Radius: {exp.blast_radius}")
            print(f"    Duration: {duration:.2f}s")
            print(f"    Result: {exp.result.upper()}")
            
            if exp.observations:
                print(f"    Observations:")
                for obs in exp.observations:
                    print(f"      - {obs}")
        
        print("\n" + "=" * 80)
        print("RESILIENCE ASSESSMENT")
        print("=" * 80)
        
        score = (len(completed) / len(self.experiments) * 100) if self.experiments else 0
        
        print(f"Resilience Score: {score:.0f}/100")
        
        if score >= 80:
            assessment = "EXCELLENT"
        elif score >= 60:
            assessment = "GOOD"
        elif score >= 40:
            assessment = "FAIR"
        else:
            assessment = "NEEDS IMPROVEMENT"
        
        print(f"Assessment: {assessment}")
        
        print("\n" + "=" * 80)
        print("RECOMMENDATIONS")
        print("=" * 80)
        print("1. Implement circuit breakers for failing operations")
        print("2. Add connection pool monitoring and alerts")
        print("3. Set query timeouts to prevent resource exhaustion")
        print("4. Implement retry logic with exponential backoff")
        print("5. Regular chaos drills to validate improvements")
        print("=" * 80)
    
    def run_chaos_suite(self):
        """Run complete chaos engineering suite"""
        
        print("\n" + "=" * 80)
        print("CHAOS ENGINEERING FRAMEWORK")
        print("=" * 80)
        
        if not self.connect():
            return
        
        self.setup()
        
        # Establish baseline
        print("\nPHASE 1: Establish Baseline")
        print("-" * 80)
        self.capture_baseline()
        
        time.sleep(2)
        
        # Experiment 1
        print("\nPHASE 2: CPU Load Experiment")
        print("-" * 80)
        self.inject_high_cpu_load()
        time.sleep(2)
        
        # Experiment 2
        print("\nPHASE 3: Connection Saturation Experiment")
        print("-" * 80)
        self.inject_connection_saturation()
        time.sleep(2)
        
        # Experiment 3
        print("\nPHASE 4: Slow Query Experiment")
        print("-" * 80)
        self.inject_slow_queries()
        time.sleep(2)
        
        # Experiment 4
        print("\nPHASE 5: Random Failure Experiment")
        print("-" * 80)
        self.inject_random_failures()
        time.sleep(2)
        
        # Recovery test
        print("\nPHASE 6: Recovery Testing")
        print("-" * 80)
        self.test_recovery_time()
        
        # Generate report
        self.generate_report()
        
        print("\n" + "=" * 80)
        print("Key Features:")
        print("  - Controlled failure injection")
        print("  - Baseline metric comparison")
        print("  - Automated resilience testing")
        print("  - Recovery time measurement")
        print("  - Comprehensive reporting")
        print("=" * 80)


def main():
    chaos = ChaosFramework()
    chaos.run_chaos_suite()


if __name__ == "__main__":
    main()
