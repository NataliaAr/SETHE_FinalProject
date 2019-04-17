sqoop export \
	--connect jdbc:mysql://localhost:3306/narinicheva \
	--username root \
	--password cloudera \
	--table most_frequently_purchased_categories \
	--export-dir '/user/cloudera/most_frequently_purchased_categories'

sqoop export \
	--connect jdbc:mysql://localhost:3306/narinicheva \
	--username root \
	--password cloudera \
	--table most_frequently_purchased_products \
	--export-dir '/user/cloudera/most_frequently_purchased_products'
	
sqoop export \
	--connect jdbc:mysql://localhost:3306/narinicheva \
	--username root \
	--password cloudera \
	--table countries_with_highest_money_spending \
	--export-dir '/user/cloudera/countries_with_highest_money_spending' 