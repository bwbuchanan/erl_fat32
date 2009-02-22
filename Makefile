all:
	@cd src && erl -make

clean:
	@rm ebin/*.beam
