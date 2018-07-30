package com.customerdna.dna.migratedata.MigrationService;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.sql.*;
import java.util.*;

@Service
public class MigrationService {
    private final Logger log = LoggerFactory.getLogger(MigrationService.class);


    @Value("${environment}")
    private String DB_url;



    public static final String DB_url1 = "jdbc:postgresql://localhost:5433/fareye_mobi_dev";
    public static final String User1 = "lav";
    public static final String Pass1 = "postgres";

    int order_no, product_desc_value, address1, address2, pincode, city, mob_no, alt_no,landmark,city_job_data,hub_job_data,consignee_name,client_name;
    ArrayList<Timestamp> order_date = new ArrayList();
    ArrayList<Integer> attempt_count = new ArrayList();
    ArrayList<Double> job_latitude = new ArrayList();
    ArrayList<Double> job_longitude = new ArrayList();
    ArrayList<Long> job_id = new ArrayList();
    ArrayList<String> company_name = new ArrayList();
    ArrayList<Timestamp> status_update_time = new ArrayList();
    ArrayList<Double> loc_latitude = new ArrayList();
    ArrayList<Double> loc_longitude = new ArrayList();
    ArrayList<Double> cash_amount = new ArrayList();
    ArrayList<String> cash_mode = new ArrayList();
    ArrayList<Integer> no_of_calls = new ArrayList();
    ArrayList<Integer> call_duration = new ArrayList();
    ArrayList<Integer> no_of_sms = new ArrayList();
    ArrayList<String> order_number = new ArrayList();
    ArrayList<String> product_desc = new ArrayList();
    ArrayList<String> cust_add = new ArrayList();
    ArrayList<String> cust_add_line1 = new ArrayList();
    ArrayList<String> cust_add_lin2 = new ArrayList();
    ArrayList<String> mobile_no = new ArrayList();
    ArrayList<Integer> job_status_category = new ArrayList();
    ArrayList<String> pincodes = new ArrayList();
    ArrayList<String> feCities = new ArrayList<>();
    ArrayList<String> feHubs = new ArrayList<>();
    ArrayList<String> job_type = new ArrayList<>();
    ArrayList<Integer> job_master_id = new ArrayList<>();
    ArrayList<Long> runsheetIdList = new ArrayList();
    ArrayList<Integer> correct = new ArrayList();
    ArrayList<String> landmarks = new ArrayList();
    ArrayList<Long> fareye_fe_id = new ArrayList();
    ArrayList<Double> gps_signal = new ArrayList();
    ArrayList<Long> fareye_city_id = new ArrayList();
    ArrayList<Long> fareye_hub_id = new ArrayList();
    ArrayList<String> citiesJobData = new ArrayList();
    ArrayList<String> hubsJobData = new ArrayList();
    ArrayList<String> consigneeNames = new ArrayList();
    ArrayList<String> clientNames = new ArrayList();
    ArrayList<String> alternateMobNos = new ArrayList();
    ArrayList<String> feSequence = new ArrayList();
    ArrayList<String> npsFeedback = new ArrayList();
    ArrayList<String> employeeCodes = new ArrayList();
    ArrayList<Double> originalAmount = new ArrayList();
    Map<String,String> hubIdMap = new HashMap<>();
    Map<String,String> cityIdMap = new HashMap<>();
    Map<String,String> userIdMap = new HashMap<>();



    int total = 0;
    Connection conn = null;
    Statement stmt = null;
    String company_id_list;

    public void migrateData(String startDate, String endDate,String prevRunsheet) {
        try {
            company_id_list = "(35)";
            Class.forName("org.postgresql.Driver");
            Connection conn1 = DriverManager.getConnection(DB_url, "fareye", "#fareye2018");
            PreparedStatement stmt1 = conn1.prepareStatement("insert into dna_data (address_line1,address_line2,landmark,contact_number,fareye_id,runsheet_number,company_name,order_number,order_date,job_lat,job_lng,last_status,last_update_time,last_update_lat,last_update_lng,actual_amount,payment_mode,call_count,call_duration,sms_count,attempt_count,pincode,job_type,time_zone,day_of_week,fareye_fe_id,gps_signal,fareye_city_id,fareye_hub_id,fareye_city_name,fareye_hub_name,city_job_data,hub_job_data,sequence,nps_feedback,courier_employee_code,client_name,consignee_name,alt_contact_number,product_desc,original_amount) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
            Properties props = new Properties();
            props.setProperty("connectTimeout", "10000"); //// This is for connect timeout because sometimes the I/O pipe breaks.
            props.setProperty("user", "fareye");
            props.setProperty("password", "#fareye2018");
            conn = DriverManager.getConnection(DB_url, props);
            stmt = conn.createStatement();
            String sql2 = "select id from runsheet where company_id in " + company_id_list + " and hub_id in (14598,1374,5047,1364,14907,14604,14612,18551,1516,1389,14614,4444,5277,14620,1539,1506,15006) and sequence_count!=0 and start_date >= \'" + startDate + "\' and end_date < \'" + endDate + "\' and id>"+prevRunsheet+" and hub_id in (14598,1374,5047,1364,14907,14604,14612,18551,1516,1389,14614,4444,5277,14620,1539,1506,15006) order by id asc";
            log.error("Querying for runsheet >>> : " + sql2);
            String sql3 = "select id,name from city where company_id=35";

            String sql4 = "select id,name from hub where company_id=35";
            String sql5 = "select id,login from users where company_id=35";
            ResultSet rs2 ;

            rs2 = stmt.executeQuery(sql2);
            while (rs2.next()) {
                runsheetIdList.add(Long.parseLong(rs2.getString("id")));
            }

            log.error("Total number of runsheet is >>> : " + runsheetIdList.size());

            rs2 = stmt.executeQuery(sql3);
            createCityMap(rs2);

            rs2 = stmt.executeQuery(sql4);
            createHubMap(rs2);

            rs2 = stmt.executeQuery(sql5);
            createUserMap(rs2);
            long previous = 0


                    ;
            for (int i = 0; i < runsheetIdList.size(); i++) {
                log.error("Extract Job Transaction for runsheet no " + runsheetIdList.get(i) + " sequence is " + i);
                pull_from_job_transaction(runsheetIdList.get(i), stmt, stmt1, previous); // Extract data for runsheet_no[i]
                previous = runsheetIdList.get(i);
            }
            stmt1.close();
            conn1.close();
            stmt.close();
            conn.close();
            rs2.close();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void createHubMap(ResultSet rs2) {
        try {
            while (rs2.next()) {
                hubIdMap.put(rs2.getString("id"), rs2.getString("name"));
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    private void createCityMap(ResultSet rs2) {
        try {
            while (rs2.next()) {
                cityIdMap.put(rs2.getString("id"), rs2.getString("name"));
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    private void createUserMap(ResultSet rs2) {
        try {
            while (rs2.next()) {
                userIdMap.put(rs2.getString("id"), rs2.getString("login"));
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }


    /*
    This function is IMPORTANT.
    All the Job types should be defined in this function with job_attribute_master_id's for address, mobile, pincode, order_no, product_desc, city
     */
    public void PresetValues(int job_master_id) {

        if (job_master_id == 157) {
            order_no = 0;
            product_desc_value = 1352;
            address1 = 1363;
            address2 = 1364;
            pincode = 1350;
            city = 0;
            mob_no = 2541;
            landmark = 1346;
            city_job_data = 1343;
            hub_job_data = 1344;
            consignee_name = 1341;
            client_name = 1340;
            alt_no = 2542;
            job_type.add("Surface Cod");
        }
        if (job_master_id == 158) {
            order_no = 0;
            product_desc_value = 1391;
            address1 = 1372;
            address2 = 1373;
            pincode = 2539;
            city = 0;
            mob_no = 2539;
            landmark = 1383;
            city_job_data = 1376;
            hub_job_data = 1377;
            consignee_name = 1374;
            client_name = 1371;
            alt_no = 2540;
            job_type.add("Surface Prepaid ");
        }
        if (job_master_id == 159) {
            order_no = 0;
            product_desc_value = 1418;
            address1 = 1399;
            address2 = 1400;
            pincode = 1415;
            city = 0;
            mob_no = 2533;
            landmark = 1410;
            city_job_data = 1403;
            hub_job_data = 1404;
            consignee_name = 1401;
            alt_no = 2534;
            client_name = 1398;
            job_type.add("Apex Cod");
        }
        if (job_master_id == 160) {
            order_no = 0;
            product_desc_value = 1445;
            address1 = 1426;
            address2 = 1427;
            pincode = 1442;
            city = 0;
            landmark = 1437;
            mob_no = 2537;
            city_job_data = 1430;
            hub_job_data = 1431;
            consignee_name = 1428;
            alt_no = 2538;
            client_name = 1425;
            job_type.add(" Document");
        }
        if (job_master_id == 161) {
            order_no = 0;
            product_desc_value = 1472;
            address1 = 1453;
            address2 = 1454;
            pincode = 1469;
            city = 0;
            landmark = 1464;
            mob_no = 2531;
            city_job_data = 1457;
            hub_job_data = 1458;
            consignee_name = 1455;
            alt_no = 2532;
            client_name = 1452;
            job_type.add("Apex Prepaid ");
        }

        if (job_master_id == 162) {
            order_no = 0;
            product_desc_value = 1491;
            address1 = 1502;
            address2 = 1503;
            pincode = 1489;
            city = 0;
            mob_no = 2535;
            landmark = 1485;
            city_job_data = 1482;
            hub_job_data = 1482;
            consignee_name = 1480;
            alt_no = 2536;
            client_name = 1479;
            job_type.add("Credit Card");
        }

        if (job_master_id == 172) { // no pincode
            order_no = 0;
            product_desc_value = 1678;
            address1 = 1690;
            address2 = 1691;
            pincode = 0;
            city = 0;
            mob_no = 2543;
            landmark = 1676;
            city_job_data = 1670;
            hub_job_data = 1671;
            consignee_name = 1668;
            alt_no = 2544;
            client_name = 1667;
            job_type.add("International");
        }

        if (job_master_id == 295) {
            order_no = 0;
            product_desc_value = 2853;
            address1 = 2864;
            address2 = 2865;
            pincode = 2851;
            city = 0;
            mob_no = 2870;
            landmark = 2867;
            city_job_data = 2845;
            hub_job_data = 2846;
            consignee_name = 2843;
            alt_no = 2871;
            client_name = 2842;
            job_type.add("Regional Priority");
        }

        if (job_master_id == 570) {  // 6429 | Is Partial Pickup           |           570 ??  no pincode
            order_no = 0;
            product_desc_value = 0;
            address1 = 6433;
            address2 = 6434;
            pincode = 0;
            city = 0;
            mob_no = 2870;
            landmark = 0;
            city_job_data = 0;
            hub_job_data = 0;
            consignee_name = 0;
            alt_no = 0;
            client_name = 0;
            job_type.add("NEW Closed");
        }
        if (job_master_id == 571) { // only contact present
            order_no = 0;
            product_desc_value = 0;
            address1 = 0;
            address2 = 0;
            pincode = 0;
            city = 0;
            mob_no = 6484;
            landmark = 0;
            city_job_data = 0;
            hub_job_data = 0;
            consignee_name = 0;
            alt_no = 0;
            client_name = 0;
            job_type.add("NEW OPEN PARTIAL");
        }

        if (job_master_id == 952) { // only contact present
            order_no = 0;
            product_desc_value = 0;
            address1 = 0;
            address2 = 0;
            pincode = 0;
            city = 0;
            mob_no = 12336;
            landmark = 0;
            city_job_data = 0;
            hub_job_data = 0;
            consignee_name = 0;
            alt_no = 0;
            client_name = 0;
            job_type.add(" RVP QC");
        }

        if (job_master_id == 953) { // only contact present
            order_no = 0;
            product_desc_value = 0;
            address1 = 0;
            address2 = 0;
            pincode = 0;
            city = 0;
            mob_no = 12387;
            landmark = 0;
            city_job_data = 0;
            hub_job_data = 0;
            consignee_name = 0;
            alt_no = 0;
            client_name = 0;
            job_type.add("RVP_MIMG");
        }
    }




//    private String addressLine1;
//    private String addressLine2;
//    private String landmark;
//    private String contactNumber;
//    private Long fareyeId;
//    private Long runsheetNumber;
//    private String courierName;
//    private String orderNumber;
//    private String productDescription;
//    private String orderDate;
//    private Double jobLat;
//    private Double jobLong;
//    private String lastStatus;
//    private String lastUpdateTime;
//    private Double lastUpdateLat;
//    private Double lastUpdateLong;
//    private Double amount;
//    private String paymentMode;
//    private Integer callCount;
//    private Integer callDuration;
//    private Integer smsCount;
//    private Integer attemptCount;
//    private String pincode;
//    private String jobType;
//    private String timeZone;
//    private Integer dayOfWeek;
//

    public void addToDB(long runsheet_no, PreparedStatement stmt) throws InterruptedException, IOException,SQLException{
        log.error("Adding Data to POSTGRES >>>");
        String status = "PENDING";
        int count = 0;
        int outof = status_update_time.size();

        for (int i = 0; i < job_id.size(); i++) {
            if (!job_status_category.get(i).equals(1) && !job_status_category.get(i).equals(4)) {
                if (job_status_category.get(i).equals(3)) {
                    status = "DELIVERED";
                }
//                else if (job_latitude.get(i) == null || job_latitude.get(i).equals(0D) || job_longitude.get(i) == null || job_longitude.get(i).equals(0D)) {
//                    continue;
//                }
                String orderDate = ("" + order_date.get(i)).replaceAll(" ", "T");
                String statusUpdate = ("" + status_update_time.get(i)).replaceAll(" ", "T");


                stmt.setString(1,cust_add_line1.get(i).replaceAll("\\p{Cc}", " ").replaceAll("\\W+", " "));
                stmt.setString(2,cust_add_lin2.get(i));
                stmt.setString(3,landmarks.get(i));
                stmt.setString(4,mobile_no.get(i));
                stmt.setLong(5,job_id.get(i));
                stmt.setLong(6,runsheet_no);
                stmt.setString(7,company_name.get(i));
                stmt.setString(8,order_number.get(i));
                stmt.setString(9,orderDate);
                stmt.setDouble(10,job_latitude.get(i));
                stmt.setDouble(11,job_longitude.get(i));
                stmt.setString(12,status);
                stmt.setString(13,statusUpdate);
                stmt.setDouble(14,loc_latitude.get(i));
                stmt.setDouble(15,loc_longitude.get(i));
                stmt.setDouble(16,cash_amount.get(i));
                stmt.setString(17,cash_mode.get(i));
                stmt.setInt(18,no_of_calls.get(i));
                stmt.setInt(19,call_duration.get(i));
                stmt.setInt(20,no_of_sms.get(i));
                stmt.setInt(21,attempt_count.get(i));
                stmt.setString(22,pincodes.get(i));
                stmt.setString(23,job_type.get(i));
                stmt.setString(24,"IST");
                stmt.setInt(25,new DateTime(status_update_time.get(i)).withZone(DateTimeZone.forID("Asia/Kolkata")).getDayOfWeek());
                stmt.setLong(26,fareye_fe_id.get(i));
                stmt.setDouble(27,gps_signal.get(i));
                stmt.setLong(28,fareye_city_id.get(i));
                stmt.setLong(29,fareye_hub_id.get(i));
                stmt.setString(30,feCities.get(i));
                stmt.setString(31,feHubs.get(i));
                stmt.setString(32,citiesJobData.get(i));
                stmt.setString(33,hubsJobData.get(i));
                stmt.setString(34,feSequence.get(i));
                stmt.setString(35,npsFeedback.get(i));
                stmt.setString(36,employeeCodes.get(i));
                stmt.setString(37,clientNames.get(i));
                stmt.setString(38,consigneeNames.get(i));
                stmt.setString(39,alternateMobNos.get(i));
                stmt.setString(40,product_desc.get(i));
                stmt.setDouble(41,originalAmount.get(i));





                stmt.addBatch();
                count++;
                total++;

            }
        }
        stmt.executeBatch();
        correct.clear();
        cust_add.clear();mobile_no.clear();
        pincodes.clear(); job_id.clear(); company_name.clear(); order_number.clear(); product_desc.clear(); order_date.clear();
        job_latitude.clear(); job_longitude.clear(); loc_latitude.clear(); loc_longitude.clear(); cash_amount.clear(); cash_mode.clear();
        no_of_calls.clear(); call_duration.clear(); no_of_sms.clear(); attempt_count.clear(); job_type.clear();
        status_update_time.clear();job_status_category.clear();job_master_id.clear();fareye_fe_id.clear();fareye_hub_id.clear();fareye_city_id.clear();gps_signal.clear();fareye_city_id.clear();
        fareye_hub_id.clear();feCities.clear();feHubs.clear();citiesJobData.clear();hubsJobData.clear();feSequence.clear();npsFeedback.clear();employeeCodes.clear();clientNames.clear();consigneeNames.clear();
        alternateMobNos.clear();product_desc.clear();cust_add_line1.clear();
        landmarks.clear();cust_add_lin2.clear();originalAmount.clear();

        log.error("Added Count: " + count + " Out Of: " + outof + " Total Added Count = " + total);
    }

    public void JSONparsing(String json) throws InterruptedException, RuntimeException {
        try {
            JSONArray obj = new JSONArray(json);
            int no_of_data = obj.length();
            String pincodeJobData = "", orderNumberJobData = "", productDescJobData = "", add1JobData = "", add2JobData = "",
                    cityJobData = "", mobNoJobData = "", altNoJobData = "",jobLandmark="",hubJobData="",clientName="",consigneeName="";
            for (int i = 0; i < no_of_data; i++) {
                JSONObject jsonObject = obj.getJSONObject(i);
                String value = jsonObject.getString("value");
                if(value == null || value.equalsIgnoreCase("")) {
                    continue;
                }

                int jobAttributeMasterId = jsonObject.getInt("job_attribute_master_id");
                if (jobAttributeMasterId == pincode) {
                    pincodeJobData = value;
                } else if (jobAttributeMasterId == order_no) {
                    orderNumberJobData = value;
                } else if (jobAttributeMasterId == product_desc_value) {
                    productDescJobData = value;
                } else if (jobAttributeMasterId == address1 && value != null && !value.equals("null")) {
                    add1JobData = value;
                } else if (jobAttributeMasterId == address2 && value != null && !value.equals("null")) {
                    add2JobData = " " + value;
                }  else if (jobAttributeMasterId == mob_no && value != null && !value.equalsIgnoreCase("")) {
                    mobNoJobData = value;
                } else if (jobAttributeMasterId == alt_no && value != null && !value.equalsIgnoreCase("")) {
                    altNoJobData = value;
                }else if (jobAttributeMasterId == landmark && value != null && !value.equalsIgnoreCase("") && !value.equals("null")) {
                    jobLandmark = value;
                }else if(jobAttributeMasterId == city_job_data && value != null && !value.equalsIgnoreCase("") && !value.equals("null")){
                    cityJobData = value;
                }else if(jobAttributeMasterId == hub_job_data && value != null && !value.equalsIgnoreCase("") && !value.equals("null")){
                    hubJobData = value;
                }else if(jobAttributeMasterId == client_name && value != null && !value.equalsIgnoreCase("") && !value.equals("null")){
                    clientName = value;
                }else if(jobAttributeMasterId == consignee_name && value != null && !value.equalsIgnoreCase("") && !value.equals("null")){
                    consigneeName = value;
                }
            }

//            String completeAddress = add1JobData + add2JobData;
//            if(completeAddress.equals("") || pincodeJobData.equals("") || (mobNoJobData.equals("") && altNoJobData.equals("")) ||
//                    ((mobNoJobData.length() < 4) && (altNoJobData.length() < 4))) {
//                throw new RuntimeException("Invalid Record for Customer DNA.");
//            }

            pincodes.add(pincodeJobData);
            order_number.add(orderNumberJobData);
            product_desc.add(productDescJobData);
            mobile_no.add(mobNoJobData);
            alternateMobNos.add(altNoJobData);
            landmarks.add(jobLandmark);
            cust_add_line1.add(add1JobData);
            cust_add_lin2.add(add2JobData);
            clientNames.add(clientName);
            consigneeNames.add(consigneeName);
            citiesJobData.add(cityJobData);
            hubsJobData.add(hubJobData);
        } catch (JSONException r) {
            r.printStackTrace();
        }
    }

    public void pull_from_job_transaction(long runsheet_no, Statement stmt, PreparedStatement stmt1, long previous) {
        try {
            db_connect_test(stmt);
            String sql = "select t.*,jo.latitude as job_latitude,jo.longitude as job_longitude,jo.created_at as order_created_at," +
                    "(select job_status.status_category from job_status where job_status.id=t.job_status_id) as order_status," +
                    "(SELECT array_to_json(array_agg(jd.*)) from job_data jd where jd.job_id = t.job_id) as job_data from job_transaction" +
                    " t left join job jo on t.job_id=jo.id where t.runsheet_id > "+previous+"and t.runsheet_id = " + runsheet_no +
//                    " and t.latitude!=0 order by runsheet_id ASC, DATE(t.last_updated_at_server) ASC, t.last_updated_at_server ASC;";
                    "  order by runsheet_id ASC, DATE(t.last_updated_at_server) ASC, t.last_updated_at_server ASC;";
            ResultSet rs = stmt.executeQuery(sql);
            while (rs.next()) {
                if(rs.getString("job_data") == null) {
                    continue;
                }
                PresetValues(Integer.parseInt(rs.getString("job_master_id")));
                try {
                    JSONparsing(rs.getString("job_data"));
                } catch(RuntimeException rte) {
                    continue;
                }
                int jobMasterId = Integer.parseInt(rs.getString("job_master_id"));
                order_date.add(rs.getTimestamp("job_created_at"));
                attempt_count.add(Integer.parseInt(rs.getString("attempt_count")));
                job_id.add(Long.parseLong(rs.getString("job_id")));
                company_name.add((jobMasterId==409 || jobMasterId==294) ? "PepperFry" : "Blue Dart");
                status_update_time.add(rs.getTimestamp("last_updated_at_server"));
                loc_latitude.add(Double.parseDouble(rs.getString("latitude")));
                loc_longitude.add(Double.parseDouble(rs.getString("longitude")));
                cash_amount.add((rs.getString("actual_amount") == null) ? 0D : Double.parseDouble(rs.getString("actual_amount")));
                cash_mode.add((rs.getString("money_transaction_type") == null) ? "NOT GIVEN" : rs.getString("money_transaction_type").replaceAll("\"", ""));
                no_of_calls.add(Integer.parseInt(rs.getString("track_call_count")));
                no_of_sms.add(Integer.parseInt(rs.getString("track_sms_count")));
                call_duration.add(Integer.parseInt(rs.getString("track_call_duration")));
                job_latitude.add((rs.getString("job_latitude") == null) ? 0D : Double.parseDouble(rs.getString("job_latitude")));
                job_longitude.add((rs.getString("job_latitude") == null) ? 0D :Double.parseDouble(rs.getString("job_longitude")));
                job_status_category.add((rs.getString("order_status") == null) ? 0 :Integer.parseInt(rs.getString("order_status")));
                job_master_id.add(Integer.parseInt(rs.getString("job_master_id")));
                fareye_fe_id.add(Long.parseLong(rs.getString("user_id")));
                gps_signal.add((rs.getString("gps_signal") == null) ? 0D :Double.parseDouble(rs.getString("gps_signal")));
                fareye_city_id.add(Long.parseLong(rs.getString("city_id")));
                fareye_hub_id.add(Long.parseLong(rs.getString("hub_id")));
                feCities.add(cityIdMap.get(rs.getString("city_id")));
                feHubs.add(hubIdMap.get(rs.getString("hub_id")));
                feSequence.add(rs.getString("seq_selected"));
                originalAmount.add((rs.getString("original_amount") == null) ? 0D : Double.parseDouble(rs.getString("original_amount")));
                npsFeedback.add(rs.getString("nps_feedback"));
                employeeCodes.add(userIdMap.get(rs.getString("user_id")));



            }
            rs.close();
            addToDB(runsheet_no,stmt1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            db_connect_test_insert(stmt,runsheet_no,stmt1,previous);
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public void db_connect_test_insert(Statement stmt, long runsheet_no ,PreparedStatement stmt1,long previous) {
        try {
            while (stmt.getConnection().isClosed()) {
                Connection conn1 = null;
                Class.forName("org.postgresql.Driver");
                conn1 = DriverManager.getConnection(DB_url, "", "");
                this.stmt = conn1.createStatement();
                stmt = this.stmt;
            }
            pull_from_job_transaction(runsheet_no,stmt,stmt1,previous);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public Statement db_connect_test(Statement stmt) {
        try {
            while (stmt.getConnection().isClosed()) {
                log.error("Remote connection found closed. Trying reconnect.");
                Class.forName("org.postgresql.Driver");
                Properties props = new Properties();
                props.setProperty("connectTimeout", "50000"); //// This is for connect timeout because sometimes the I/O pipe breaks.
                props.setProperty("user", "fareye");
                props.setProperty("password", "winteriscoming");
                conn = DriverManager.getConnection(DB_url, props);
                stmt = conn.createStatement();
            }
            log.error("Remote connection recovered.");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return stmt;
    }

}
