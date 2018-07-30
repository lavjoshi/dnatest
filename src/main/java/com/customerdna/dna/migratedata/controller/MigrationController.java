package com.customerdna.dna.migratedata.controller;

import com.customerdna.dna.migratedata.MigrationService.MigrationService;
import org.json.JSONException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.soap.SOAPException;
import javax.xml.transform.TransformerException;
import java.io.IOException;
import java.text.ParseException;

@RestController
public class MigrationController {

    @Autowired
    MigrationService migrationService;

    @RequestMapping(value = "/api/migrate_data",
            method = RequestMethod.GET)
    public ResponseEntity deleteDSPROD(@RequestParam String startDate,@RequestParam String endDate,@RequestParam String prevRunsheetId) throws JSONException, SOAPException, TransformerException, IOException, ParserConfigurationException, ParseException {
        migrationService.migrateData(startDate,endDate,prevRunsheetId);
        return new ResponseEntity("Success", HttpStatus.OK);
    }
}
