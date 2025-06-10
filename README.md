# Goal of the project
Utilize OOP (Object-Oriented Programming) with Python to create a Data Engineering project that grabs data from a PostgreSQL database, save CSV files locally and then upload those files to AWS S3.
Why to do this?
I have found that there is very little content out there regarding the use of OOP into Data Engineering. If the discussion was about the use of OOP in Software Engineering or Web Devlopment, there are tons of mountains of content to use from. I was eager to find some books on this but no luck either. Most of the cases I have found that the gold standard principles of Software Engineering can be adapted to Data Engineering so, this is an attemp to do that and invite Data Engineers to include it in their projects too.

### Advantages of OOP in DE
1. Modularity and Reusability
You can create reusable classes like DataIngestor, DataTransformer, or DataValidator. Once you write them, you can easily plug them into different pipelines or projects with minimal changes.

2. Scalability
OOP makes it easier to scale your architecture. Need to support a new data source or destination? Just extend a base class with a new child class (inheritance) and override only the parts you need to tweak.

3. Clean Code and Maintenance
Encapsulation keeps logic tidy and separated. When your transformation logic is tucked inside a method like .clean_dates() or .normalize_columns(), it's easier to debug and test individual parts without breaking the whole system.

4. Testing and CI/CD Friendly
With well-structured classes and methods, you can write targeted unit tests for each step in your pipeline. That’s gold for CI/CD setups and avoiding regressions.

5. Intuition and Collaboration
For larger teams or when working across squads, OOP makes your code self-documenting. Anyone reading your class structure can follow the data flow like a story: ingest, clean, transform, and load.

