PHONY: clean-dir copy-data go

clean-dir:
	rm -rf output
	mkdir output

copy-data:
	cp output/data_mandaty.csv	/Users/michalkollar/Desktop/Coding/Dataviz/mandaty-web/src/assets/data/
	cp output/data_politicians.csv	/Users/michalkollar/Desktop/Coding/Dataviz/mandaty-web/src/assets/data/
	cp output/data_mandaty.csv	/Users/michalkollar/Desktop/Coding/Dataviz/mandates-web/src/assets/data/
	cp output/data_politicians.csv	/Users/michalkollar/Desktop/Coding/Dataviz/mandates-web/src/assets/data/

go:
	make clean-dir

	python3 fetch_load_parties.py
	python3 fetch_load_popularity.py
	python3 extract_all.py

	make copy-data