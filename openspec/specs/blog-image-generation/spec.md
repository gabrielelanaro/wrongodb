# blog-image-generation Specification

## Purpose
TBD - created by archiving change add-agentic-image-loop. Update Purpose after archive.
## Requirements
### Requirement: Single-shot generation by default
The tool SHALL generate one image from the provided prompt when agentic mode is not enabled.

#### Scenario: Default single-shot
- **WHEN** the user runs `generate_image.py` without `--agentic`
- **THEN** the tool sends the provided prompt directly to the image model and writes exactly one image

### Requirement: Agentic loop with critique and iteration cap
The tool SHALL support an agentic loop when `--agentic` is enabled, consisting of draft → generate → critique → revise, with a configurable iteration cap.

#### Scenario: Iterative refinement
- **WHEN** the user runs with `--agentic --iterations 3`
- **THEN** the tool performs up to 3 critique/refine cycles and stops early if the critique passes

#### Scenario: Cap reached
- **WHEN** the critique does not pass within the iteration cap
- **THEN** the tool returns the last generated image and emits the failing critique summary

### Requirement: Critique rubric output
The critique step SHALL evaluate story effectiveness, visual consistency, and pleasing aesthetics, and output a structured pass/fail summary with actionable revision guidance.

#### Scenario: Critique output includes rubric
- **WHEN** an image is critiqued in agentic mode
- **THEN** the tool outputs pass/fail results for the three rubric dimensions and a revision prompt

### Requirement: Agentic sidecar summary
In agentic mode, the tool SHALL emit a sidecar JSON file containing the seed prompt, final prompt, and critique summaries for each iteration.

#### Scenario: Sidecar created
- **WHEN** agentic mode finishes
- **THEN** a sidecar JSON file is written next to the output image with prompts and critique summaries

### Requirement: Draft prompt structure
The draft step SHALL expand the seed prompt to include story purpose, key visual elements, labels/icons, and style consistency notes.

#### Scenario: Draft expansion
- **WHEN** agentic mode starts from a seed prompt
- **THEN** the generated draft prompt includes story purpose, visual elements, labels/icons, and style constraints

