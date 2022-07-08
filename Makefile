all:
	python3 -m build
	python3 -m twine upload --repository pypi dist/*
	rm -r dist build
