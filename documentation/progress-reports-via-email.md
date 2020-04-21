# Progress Reports via Email

Exoskeleton can send email
* when it starts,
* when it reaches a milestone
* after it stops.


## Technical Prerequisites

**At the current state, exoskeleton does require a Linux system with a mailserver on the same machine.** Support for other solutions is planned, see issue #14.

Note, that it usually does not work to send email from a system with a dynamic IP address (typically your home computer) as most mail servers will classify them as spam.

Even if you send from a machine with static IP, many things might go wrong. For example, there might be a [SPF setting](https://en.wikipedia.org/wiki/Sender_Policy_Framework "Wikipedia explaining the Sender Policy Framework") for the sending domain that limits sending mails to a server specified in the MX record of your DNS entry.

## Activating Mails


When you create your bot, you can set the parameter `mail_settings` which expects a dictionary. The following keys are supported:

* **mail_server** (default: 'localhost'): at the moment always localhost (see #14)
* **mail_admin** (default: None): email address of the recipient
* **mail_sender** (default: None): email address of the sender
* **send_start_msg** (default: True): whether to send a start message
* **send_finish_msg** (default: False): send a message when the bot finishes (requires that is set up to finish once the queue is empty).
* **milestone_num** (default: None): Send a mail once a certain number of tasks has been handled.

Obviously `mail_admin` and `mail_sender` are required to send email.

`send_start_msg` defaults to True, so the bot will right away send an email. This is a good way to check whether sending a mail does work. If the receiving mail server uses [greylisting](https://en.wikipedia.org/wiki/Greylisting "Wikipedia on this method to reduce spam by introducing wait time for unknown senders"), it may initially take some minutes to get this email.

> :arrow_right: **[Work with labels](labels.md)**