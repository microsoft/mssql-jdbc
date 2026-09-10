## Feature Requests

Developers who raise feature requests are encouraged to implement the enhancement themselves and submit a Pull Request. The mssql-jdbc team plans its work based on priorities and pre-planned activities, and as such cannot commit guaranteed bandwidth for reviewing and testing community-driven feature enhancements. However, the team will make best efforts to assist contributors, provide guidance, and — where a feature request PR meets all quality and performance standards — align its delivery with the project's release plans.

If you are implementing a feature request:
- Open an issue first to discuss the proposed enhancement and get early feedback from the team before investing significant effort.
- Ensure the implementation meets all [coding guidelines](coding-guidelines.md), [coding best practices](coding-best-practices.md), and [review process](review-process.md) requirements.
- Include comprehensive tests covering the new functionality.
- Verify that performance benchmarks are not negatively impacted.

---

- **DO** report each issue as a new issue (but check first if it's already been reported)
- **DO** respect Issue Templates and provide detailed information. It will make the process to reproduce the issue and provide a fix faster.
- **DO** provide a minimal repro app demonstrating the problem in isolation will greatly speed up the process of identifying and fixing problems.
- **DO** follow our [coding guidelines](coding-guidelines.md) when working on a Pull Request.
- **DO** follow our [coding best practices](coding-best-practices.md) when working on a Pull Request.
- **DO** follow our [review process](review-process.md) when reviwing a Pull Request.
- **DO** give priority to the current style of the project or file you're changing even if it diverges from the general guidelines.
- **DO** consider cross-platform compatibility and supportability for all supported SQL and Azure Servers and client configurations.
- **DO** include tests when adding new features. When fixing bugs, start with adding a test that highlights how the current behavior is broken.