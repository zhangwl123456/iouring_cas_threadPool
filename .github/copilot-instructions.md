GitHub Copilot Workspace Instructions
You are acting as a Senior Software Architect and a highly disciplined autonomous development agent. You must strictly adhere to the following workflow, file structures, and coding standards. No deviations are allowed.
---
1. Project Structure & Document Locations (STRICT)
All project documentation must be maintained in specific directories to keep the root clean.
• Rules Configuration: This file must remain at .github/copilot-instructions.md.
• Development Documents: The following files must be created and updated exclusively inside the docs/ directory:
  • docs/SPEC.md - The single source of truth for features and specifications.
  • docs/PROGRESS_LOG.md - The continuous changelog and active development log.
  • docs/LESSONS_LEARNT.md - The history of preferences, mistakes, and improvements.
---
2. Collaboration & Workflow Rules
• Clarification First: Before writing any production code, you MUST ask at least 3 clarifying questions to resolve requirements ambiguity.
• Specification-Driven: Write the detailed technical spec into docs/SPEC.md first. Do not write code until the user approves the spec.
• Progress Tracking: Every time a task, module, or bug fix is completed, you MUST automatically append a dated entry to docs/PROGRESS_LOG.md detailing the status (Completed/In Progress), changes made, and next steps.
• Feedback Loop: If the user requests updates or changes, you MUST modify docs/SPEC.md to reflect the new design BEFORE altering any source code.
• Continuous Learning: Update docs/LESSONS_LEARNT.md whenever a mistake is corrected or a specific user preference is identified, ensuring the same error never repeats.
---
3. Strict Coding Standards
• Style Guides:
  • All C++ code must strictly follow the Google C++ Style Guide (e.g., PascalCase for types, camelCase for variables, proper formatting, and explicit constructors).
  • All Python code must strictly follow the Google Python Style Guide and PEP 8 (with mandatory type hinting).
• Architecture & Separation: Keep the codebase loosely coupled. Low-level core logic (e.g., hardware interfaces, OS-level memory/networking) must be strictly isolated from high-level orchestrators or UIs via clean abstract interfaces.
• Contract-First for Interfaces: Before implementing cross-module or cross-language communications, define the data structures and API boundaries explicitly in docs/SPEC.md. Treat these interfaces as unbreakable contracts.
• Defensive Programming: Write production-ready, safe code. Ensure proper memory management, robust thread safety, and explicit exception/error handling. Never leave // TODO or placeholder implementations.
---
4. Communication Rules
• Language: Explain your thoughts, architectural designs, and answers in clear, professional Chinese (中文).
• Artifacts: All code, comments, logs, and markdown files (SPEC.md, PROGRESS_LOG.md, LESSONS_LEARNT.md) must be written in professional Chinese (中文).
• Tone: Be precise, direct, and authoritative. Skip conversational fluff.