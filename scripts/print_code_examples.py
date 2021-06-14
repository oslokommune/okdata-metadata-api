import argparse
import os

_PYGMENTS_INSTALLED = False
try:
    import pygments
    from pygments.lexers import get_lexer_by_name
    from pygments.formatters import get_formatter_by_name

    _PYGMENTS_INSTALLED = True
except ImportError:
    pass

# Must be done before repository import.
os.environ["AWS_XRAY_SDK_ENABLED"] = "false"

from metadata.dataset.code_examples import code_examples, content_types  # noqa: E402

_FORMATS = {"source": "py"}
if _PYGMENTS_INSTALLED:
    _FORMATS.update({"html": "html"})


def write_file(code, fmt, outfile):
    with open(outfile, "w") as f:
        if fmt == "source":
            f.write(code)
        elif fmt == "html":
            pygments.highlight(
                code=code,
                lexer=get_lexer_by_name("python3"),
                formatter=pygments.formatters.HtmlFormatter(
                    full=True,
                ),
                outfile=f,
            )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate dataset code examples. Install `pygments` for additional functionality."
    )
    parser.add_argument("-e", "--env", required=True, choices=["dev", "prod"])
    parser.add_argument(
        "-d",
        "--dataset-id",
        required=True,
    )
    parser.add_argument(
        "-f",
        "--format",
        action="append",
        choices=_FORMATS,
        required=False,
        default=[],
        help="Optional. Write formatted code to file.",
    )
    parser.add_argument(
        "-o",
        "--output-path",
        required=False,
        default="/tmp/examples",
        help="Optional. Path to directory where you want output to be written.",
    )

    args = parser.parse_args()
    os.environ["AWS_PROFILE"] = f"okdata-{args.env}"
    dataset_id = args.dataset_id

    examples = code_examples(dataset_id=args.dataset_id)

    for i, example in enumerate(examples):
        content_type = content_types.get(example["content_type"])
        print()
        print("=" * 50)
        print(f"Dataset: {dataset_id}")
        print(f"Content type: {example['content_type']}")
        print("-" * 50)
        code = example["code"]
        print(
            pygments.highlight(
                code=code,
                lexer=get_lexer_by_name("python3"),
                formatter=get_formatter_by_name("terminal"),
            )
            if _PYGMENTS_INSTALLED
            else code
        )

        for fmt in args.format:
            if fmt != "source" and not _PYGMENTS_INSTALLED:
                continue

            if not os.path.exists(args.output_path):
                os.makedirs(args.output_path)

            filename = (
                f"{dataset_id}_{content_type}.{_FORMATS[fmt]}"
                if content_type
                else f"{dataset_id}.{_FORMATS[fmt]}"
            )
            outfile = os.path.join(args.output_path, filename)

            write_file(code, fmt, outfile)
