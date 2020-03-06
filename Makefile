.PHONY: pyclean doc test html

pyclean:
	find . -type f -name "*.py[co]" -delete
	find . -type d -name "__pycache__" -delete
	rm -rf build/ dist/ *.egg-info

doc:
	python bin/generate_cli_documentation.py
	pydocmd simple substra.sdk+ substra.sdk.Client+ > references/sdk.md

test: pyclean
	python setup.py test

html:
	echo "in makefile / html"
	sphinx-build -b html -d _build/doctrees . _build/html