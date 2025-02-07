#!/usr/bin/env python3
import os
import json
from analyzer.core.log_analyzer import SparkLogAnalyzer
from analyzer.core.report_generator import ReportGenerator

def analyze_debug_info(debug_dir):
    print(f"Analyzing debug information from: {debug_dir}")
    
    # Initialize analyzer with debug directory
    analyzer = SparkLogAnalyzer(debug_dir)
    
    # Perform analysis
    findings = analyzer.analyze_all()
    
    # Generate detailed report
    report_gen = ReportGenerator(findings)
    report = report_gen.generate_markdown_report()
    
    # Save report
    report_file = os.path.join(debug_dir, 'analysis_report.md')
    with open(report_file, 'w') as f:
        f.write(report)
    
    # Save raw findings
    findings_file = os.path.join(debug_dir, 'analysis_findings.json')
    with open(findings_file, 'w') as f:
        json.dump(findings, f, indent=2)
    
    return report_file, findings_file

if __name__ == "__main__":
    debug_dir = "collected_info_20250207_021735"
    report_file, findings_file = analyze_debug_info(debug_dir)
    print(f"\nAnalysis complete!")
    print(f"Report saved to: {report_file}")
    print(f"Raw findings saved to: {findings_file}")
