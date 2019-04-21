import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Review {
    private String review_id;
    private Double longitude;
    private Double latitude;
    private Double altitude;
    private String review_date;
    private String temperature;
    private Double rating;
    private String user_id;
    private String birthday;
    private String user_nationality;
    private String career;
    private Double user_income;
    private static final Map<String, String> MONTHS;
    static {
        MONTHS = new HashMap<>();
        MONTHS.put("january", "01");
        MONTHS.put("february", "02");
        MONTHS.put("march", "03");
        MONTHS.put("april", "04");
        MONTHS.put("may", "05");
        MONTHS.put("june", "06");
        MONTHS.put("july", "07");
        MONTHS.put("august", "08");
        MONTHS.put("september", "09");
        MONTHS.put("october", "10");
        MONTHS.put("november", "11");
        MONTHS.put("december", "12");
    }

    Review(String record) {
        String[] items = record.split("\\|");
        review_id = items[0];
        try {
            longitude = Double.parseDouble(items[1]);
        } catch (NumberFormatException e) {
            longitude = null;
        }
        try {
            latitude = Double.parseDouble(items[2]);
        } catch (NumberFormatException e) {
            latitude = null;
        }
        try {
            altitude = Double.parseDouble(items[3]);
        } catch (NumberFormatException e) {
            altitude = null;
        }
        review_date = standardDate(items[4]);
        temperature = standardTemperature(items[5]);
        try {
            rating = normalizeRating(Double.parseDouble(items[6]));
        } catch (NumberFormatException e) {
            rating = (double) -1;
        }
        user_id = items[7];
        birthday = standardDate(items[8]);
        user_nationality = items[9];
        career = items[10];
        try {
            user_income = Double.parseDouble(items[11]);
        } catch (NumberFormatException e) {
            user_income = (double) -1;
        }
    }

    /**
     * @return the career
     */
    public String getCareer() {
        return career;
    }

    /**
     * @return the user_income
     */
    public Double getUser_income() {
        return user_income;
    }

    /**
     * @param user_income the user_income to set
     */
    public void setUser_income(Double user_income) {
        this.user_income = user_income;
    }

    /**
     * @param rating the rating to set
     */
    public void setRating(Double rating) {
        this.rating = rating;
    }

    boolean validateLongitude() {
        return longitude != null && longitude > 8.1461259 && longitude < 11.1993265;
    }

    boolean validateLatitude() {
        return latitude != null && latitude > 56.5824856 && latitude < 57.750511;
    }

    String standardTemperature(String temperature) {
        Pattern temperatureFormat1 = Pattern.compile("(.*)℉");
        Pattern temperatureFormat2 = Pattern.compile("(.*)℃");
        Matcher matcher;
        matcher = temperatureFormat1.matcher(temperature);
        if (matcher.find()) {
            Double number = Double.parseDouble(matcher.group(1));
            return "" + ((number - 32) / 1.8) + "℃";
        }
        matcher = temperatureFormat2.matcher(temperature);
        if (matcher.find()) {
            return temperature;
        }
        return null;
    }

    String standardDate(String date) {
        Pattern dateFormat1 = Pattern.compile("(\\d{4})/(\\d{2})/(\\d{2})");
        Pattern dateFormat2 = Pattern.compile("(\\d{4})-(\\d{2})-(\\d{2})");
        Pattern dateFormat3 = Pattern.compile("(.*)\\s+(\\d{2}),\\s*(\\d{4})");
        Pattern dateFormat4 = Pattern.compile("(.*)\\s+(\\d{1}),\\s*(\\d{4})");
        Matcher matcher;
        matcher = dateFormat1.matcher(date);
        if (matcher.find()) {
            return matcher.group(1) + "-" + matcher.group(2) + "-" + matcher.group(3);
        }
        matcher = dateFormat2.matcher(date);
        if (matcher.find()) {
            return date;
        }
        matcher = dateFormat3.matcher(date);
        if (matcher.find()) {
            return matcher.group(3) + "-" + MONTHS.get(matcher.group(1).toLowerCase()) + "-" + matcher.group(2);
        }
        matcher = dateFormat4.matcher(date);
        if (matcher.find()) {
            return matcher.group(3) + "-" + MONTHS.get(matcher.group(1).toLowerCase()) + "-0" + matcher.group(2);
        }
        return null;
    }

    Double normalizeRating(Double rating) {
        return rating / 100;
    }

    /**
     * @return the rating
     */
    public Double getRating() {
        return rating;
    }

    @Override
    public String toString() {
        return review_id + "|" + longitude + "|" + latitude + "|" + altitude + "|" + review_date + "|" + temperature
                + "|" + rating + "|" + user_id + "|" + birthday + "|" + user_nationality + "|" + career + "|"
                + user_income;
    }
}