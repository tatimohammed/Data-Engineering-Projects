import plSQLGen as plg

if __name__ == "__main__":
    generator = plg.PlSQL_Generator()

    file = "input1.csv"

    generator.generatePlSQL(file)
