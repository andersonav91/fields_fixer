package com.andersonav91.integration.datacorrector;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;


import java.io.*;
import java.time.Instant;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Stream;

@SpringBootApplication
public class DataCorrectorApplication implements CommandLineRunner {

	private static Logger LOG = LoggerFactory
			.getLogger(DataCorrectorApplication.class);

	public static void main(String[] args) {
		SpringApplication.run(DataCorrectorApplication.class, args);
	}

	public String[][] complementaryData(){
		String[][] returnData = { };
		final String filePath = "E:\\Downloads\\Demo1.csv";
		try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
			String line;
			while ((line = br.readLine()) != null) {
				returnData = ArrayUtils.add(returnData, line.split(","));
			}
		} catch (IOException ioException) {
			LOG.info("File " + filePath + "not found");
		}
		return returnData;
	}

	public String[] findSenderIntFile(String value, String[][] searchInArray) {
		Stream result = Arrays.stream(searchInArray)
				.filter(x -> x[0].equals(value.toString()));
		Optional<String[]> firstMatch = result.findFirst();
		if (firstMatch.isPresent()) {
			return firstMatch.get();
		}
		return new String[]{"", "", "", ""};
	}

	@Override
	public void run(String... args) {
		final String finalFilePath = "E:\\Downloads\\Demo.txt";
		String header = "";
		String[][] complementaryData = this.complementaryData();
		try (BufferedReader br = new BufferedReader(new FileReader(finalFilePath))) {
			String line;

			String[][] invalidLines = {};
			int lineNumber = 0, firstNameIndex = 0, lastNameIndex = 0, middleNameIndex = 0;

			while ((line = br.readLine()) != null) {
				String[] fields = line.split("\\|");
				if(lineNumber == 0){
					header = line;
					String[] columns = {"first_name", "last_name", "middle_name"};
					for(int i = 0; i < fields.length; i ++) {
						if(Arrays.asList(columns).contains(fields[i])){
							LOG.info("Index for " + fields[i] +  " field = " + i);
						}
						if (fields[i].equals("first_name")) { firstNameIndex = i;}
						if (fields[i].equals("last_name")) { lastNameIndex = i;}
						if (fields[i].equals("middle_name")) { middleNameIndex = i;}
					}
				} else {
					String senderIntFile = fields[0];
					String firstName = fields[firstNameIndex];
					String middleName = fields[middleNameIndex];
					String lastName = fields[lastNameIndex];

					String[] newData = this.findSenderIntFile(senderIntFile, complementaryData);

					if ((! firstName.equalsIgnoreCase(newData[1]) ||
							! lastName.equalsIgnoreCase(newData[3]) ||
							! middleName.equalsIgnoreCase(newData[2])) && ! StringUtils.isEmpty(newData[0])) {
						fields[firstNameIndex] = newData[1]; // first name
						fields[middleNameIndex] = newData[2]; // middle name
						fields[lastNameIndex] = newData[3]; // last name
						invalidLines = ArrayUtils.add(invalidLines, fields);
					} else if ((StringUtils.isEmpty(firstName) ||
							StringUtils.isEmpty(lastName) ||
							StringUtils.isEmpty(middleName) &&
									! StringUtils.isEmpty(newData[0]))) {

						fields[firstNameIndex] = newData[1]; // first name
						fields[lastNameIndex] = newData[3]; // last name
						boolean isInvalidMiddleName = newData[2].equalsIgnoreCase("NULL");
						boolean middleNameFlag = false;
						boolean namesFlag = false;

						if (! isInvalidMiddleName &&
							StringUtils.isEmpty(middleName)) {
							fields[middleNameIndex] = newData[2];
							middleNameFlag = true;
						}

						if (StringUtils.isEmpty(firstName) ||
							StringUtils.isEmpty(lastName)) {
							namesFlag = true;
						}

						if (namesFlag || middleNameFlag) {
							invalidLines = ArrayUtils.add(invalidLines, fields);
						}
					} else if (firstName.equalsIgnoreCase(newData[1]) &&
							lastName.equalsIgnoreCase(newData[3]) &&
							middleName.equalsIgnoreCase(newData[2]) &&
							! StringUtils.isEmpty(newData[0])) {
						invalidLines = ArrayUtils.add(invalidLines, fields);
					}
				}
				lineNumber ++;
			}
			int totalMissingRows = 0;
			for(int j = 0; j < complementaryData.length; j ++){
				String row = complementaryData[j][0];
				String missingRow[] = this.findSenderIntFile(row, invalidLines);
				if (missingRow[0].equals("") && totalMissingRows != 0) {
					LOG.info("Row with ID " + row + " wasn't loaded.");
					totalMissingRows ++;
				}
			}
			LOG.info("Total missing rows = " + totalMissingRows);
			LOG.info("Total loaded rows = " + invalidLines.length);

			try (PrintWriter out = new PrintWriter(new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream("data" + Instant.now().toEpochMilli() + ".txt", true))))) {
				LOG.info("Writing file.");
				out.println(header);
				for(int i = 0; i < invalidLines.length; i ++) {
					out.println(StringUtils.join(invalidLines[i], "|"));
					LOG.info("Writing line " + i + ".");
				}
				out.close();
				LOG.info("File was written");
			} catch (IOException e) {
				LOG.info("Error writing the new file.");
			}

		} catch (IOException ioException) {
			LOG.info("File " + finalFilePath + "not found");
		}
	}
}
