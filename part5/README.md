# Complex Pipelines in Go (Part 5): Putting it All Together

[Link to official post](https://www.mariocarrion.com/2020/08/27/go-implementing-complex-pipelines-part-5.html)

To run the program make sure you have a database running locally, for example with docker:

```
docker run -d --rm \
  -e POSTGRES_HOST_AUTH_METHOD=trust -e POSTGRES_DB=complex_pipeline \
  -p 5432:5432 --name complex_pipeline_5 postgres:12.3-alpine
```

Then, create a table by connecting to the running container:

```
docker exec -it complex_pipeline_5 psql -U postgres -d complex_pipeline
```

and executing the following SQL statement:

```
CREATE TABLE "public"."names" (
	"nconst"              varchar(255),
	"primary_name"        varchar(255),
	"birth_year"          varchar(4),
	"death_year"          varchar(4) DEFAULT '',
	"primary_professions" varchar[],
	"known_for_titles"    varchar[]
);
```

Finally run the program:

```
DATABASE_URL="postgres://postgres:@127.0.0.1:5432/complex_pipeline" go run main.go
```

The program also supports the following two parameters to control the implementation.

```
  -size int
    	batch size (default 100000)
  -timeout int
    	timeout in minutes (default 20)
```
