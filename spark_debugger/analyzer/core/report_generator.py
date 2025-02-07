import json
from datetime import datetime

class ReportGenerator:
    def __init__(self, findings):
        self.findings = findings
    
    def generate_markdown_report(self):
        report = []
        report.append("# Spark Job Analysis Report")
        report.append(f"Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        
        # Error Analysis
        report.append("## Error Analysis")
        if self.findings['error_analysis'].get('errors'):
            for i, error in enumerate(self.findings['error_analysis']['errors'], 1):
                report.append(f"\n### Error {i}")
                report.append(f"```\n{error}\n```")
        
        # Resource Usage
        report.append("\n## Resource Usage")
        if self.findings['resource_usage'].get('memory'):
            report.append("\n### Memory Usage")
            report.append(f"```\n{self.findings['resource_usage']['memory']}\n```")
        
        if self.findings['resource_usage'].get('cpu'):
            report.append("\n### CPU Usage")
            report.append(f"```\n{self.findings['resource_usage']['cpu']}\n```")
        
        # Configuration Analysis
        report.append("\n## Configuration Analysis")
        if self.findings['configuration_issues'].get('spark_configs'):
            report.append("\n### Spark Configurations")
            report.append(f"```\n{self.findings['configuration_issues']['spark_configs']}\n```")
        
        # Recommendations
        report.append("\n## Recommendations")
        for rec in self.findings['recommendations']:
            report.append(f"- {rec}")
        
        return "\n".join(report)

