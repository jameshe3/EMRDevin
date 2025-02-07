#!/usr/bin/env python3
from analyzer.core.log_analyzer import SparkLogAnalyzer
from analyzer.core.report_generator import ReportGenerator

def main():
    debug_dir = "collected_info_20250207_021735"
    
    # Analyze the debug information
    analyzer = SparkLogAnalyzer(debug_dir)
    findings = analyzer.analyze_all()
    
    # Generate report
    report_gen = ReportGenerator(findings)
    report = report_gen.generate_markdown_report()
    
    # Save report
    with open('analysis_report.md', 'w') as f:
        f.write(report)
    
    print("Analysis complete. Report saved to analysis_report.md")

if __name__ == "__main__":
    main()
