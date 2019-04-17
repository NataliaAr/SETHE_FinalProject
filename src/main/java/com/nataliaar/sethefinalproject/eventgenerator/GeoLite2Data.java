package com.nataliaar.sethefinalproject.eventgenerator;

import java.io.IOException;
import java.io.Reader;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;

public class GeoLite2Data {
    private List<String> networkData = new ArrayList<String>();

    public GeoLite2Data(URI fileUri) throws IOException {
        readData(createCsvReader(fileUri));
    }

    private void readData(CSVReader csvReader) throws IOException {
        String[] line;

        while ((line = csvReader.readNext()) != null) {
            if (line.length > 0) {
                networkData.add(line[0]);
            }
        }

        csvReader.close();
    }

    private CSVReader createCsvReader(URI fileUri) throws IOException {
        CSVParser parser = new CSVParserBuilder().withSeparator(',').build();

        Reader reader = Files.newBufferedReader(Paths.get(fileUri));

        return new CSVReaderBuilder(reader).withSkipLines(1).withCSVParser(parser).build();
    }

    public String getNetworkData(int index) {
        return networkData.get(index);
    }

    public int getNetworkDataSize() {
        return networkData.size();
    }
}
