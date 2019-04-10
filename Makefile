build:
	mkdir -p build
test: build
	mkdir -p build/test
test/liboffs: test liboffs/*.pony liboffs/test/*.pony
	stable fetch
	stable env ponyc liboffs/test -o build/test --debug
test/execute: test/liboffs
	./build/test/test
clean:
	rm -rf build

.PHONY: clean test
