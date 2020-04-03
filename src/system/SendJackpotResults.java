/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package system;

import java.util.Properties;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import javax.mail.PasswordAuthentication;
import utils.Logging;
import utils.Props;

/**
 *
 * @author karuri
 */
public class SendJackpotResults {

    public SendJackpotResults() {
    }

    public void sendMail(String msg, String sub) {
        
        Properties props = new Properties();
        props.put("mail.smtp.host", "smtp.gmail.com");
        props.put("mail.smtp.socketFactory.port", "465");
        props.put("mail.smtp.socketFactory.class", "javax.net.ssl.SSLSocketFactory");
        props.put("mail.smtp.auth", "true");
        props.put("mail.smtp.port", "465");

        Session session = Session.getDefaultInstance(props, new javax.mail.Authenticator() {
            @Override
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication("rubewafula@gmail.com", "0tuma@01");
            }
        });

        try {
            MimeMessage message = new MimeMessage(session);
            InternetAddress[] iAdressArray = InternetAddress.parse(Props.getUserEmails());
            message.addRecipients(Message.RecipientType.CC, iAdressArray);
            message.setSubject(sub);
            message.setText(msg);

            Transport.send(message);
        } catch (MessagingException e) {
            Logging.error("Error thrown sending message", e);
        }

    }

}
