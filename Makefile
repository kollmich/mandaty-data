PHONY: clean-dir copy-data go

clean-dir:
	rm -rf output
	mkdir output

copy-data:
	cp output/data_mandaty.csv	/Users/michalkollar/Desktop/Coding/Dataviz/mandaty-web/src/assets/data/

go:
	make clean-dir

	python3 load.py
	python3 extract.py

	make copy-data