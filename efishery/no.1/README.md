# DWH alternative scheme

https://dbdiagram.io/d/6336ae027b3d2034fff4807c

because based on my opinion a great DWH scheme, the fact table can only built by dependency dim table. when we're using date
as our dimensional table, we're still need to take a dependency table from other tables and that will causing
a lack of effectiviness for our DWH scheme.

fact will took id from it dimensional and accumulated based on our needs in fact table itself.