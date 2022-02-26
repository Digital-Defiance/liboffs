build:
	mkdir -p build
test: build
	mkdir -p build/test
test/liboffs: test liboffs/test/*.pony
	corral fetch
	corral run -- ponyc liboffs/test -o build/test --debug
test/execute: test/liboffs
	./build/test/test --sequential
clean:
	rm -rf build

.PHONY: clean test
