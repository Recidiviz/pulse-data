ingest_info Field Documentation
===============================

Overview
--------

An ingest_info holds information about an individual that will be ingested into the system. This is the documentation of the available data fields, which defines the data that can be input into the system.

Value format notes:
~~~~~~~~~~~~~~~~~~~

-  All data fields are optional.

-  All data fields are stored as strings.

-  The provided strings will be converted to the appropriate data types during the ingest progress, with errors surfaced if a value is provided that cannot be appropriately converted.

-  Certain fields are stored as enumerated types: the values of these fields are taken from a pre-specified set of values. Many common ways of representing a value can be converted into the valid values listed below (e.g., ‘F’, ‘Fem’, and ‘Female’ will each result in a value of ‘Female’ for gender).

-  Values are case insensitive.

Person
------

The ingest_info object is just a container that holds a list of Person objects. Each Person holds details about the individual, as well as a list of Booking objects holding available data in the times a person has been booked into a jail.

====================== =============================================================================================================================================================================================================================
**Field Name**         **Description**
====================== =============================================================================================================================================================================================================================
**person_id**          Unique identifier for an individual. If not specified, one will be generated automatically.
**full_name**          A person’s name.

                       Only use this when names are in a single field. Use surname and given_names when they are separate.
**surname**            A person’s surname.

                       Only use this when surname and given names are in separate fields. Use full_name when they are in a single field.
**given_names**        A person’s given names, separated by whitespace.

                       Only use this when surname and given names are in separate fields. Use full_name when they are in a single field
**middle_names**       A person’s middle name(s) or initial.

                       Only use this when a middle names or initials are given in a separate field. Use full_name when they are in a single field
**name_suffix**        A person’s name suffix(s).

                       Only use this when a suffix is given in a separate field. Use full_name when they are in a single field
**birthdate**          Date the person was born.

                       Use this when it is known. When a person’s age but not birthdate is reported, use age instead.
**gender**             A person’s gender.

                       Valid values:

                       -  Female

                       -  Male

                       -  Trans Female / Trans Woman

                       -  Trans Male / Trans Man

                       -  Other

                       `Complete list of valid values <https://github.com/Recidiviz/pulse-data/blob/main/recidiviz/common/constants/person.py#L56>`__
**age**                A person’s age. From this age, a “birthdate” is inferred as the 1st of January of the year indicated by the age.

                       Use this when age is known but birthdate is not. When a birthdate is reported, use birthdate instead. Age is ignored if birthdate is set.
**race**               A person’s reported race.

                       Valid values:

                       -  American Indian / Alaskan Native

                       -  Asian

                       -  Black

                       -  Native Hawaiian / Pacific Islander

                       -  White

                       -  Other

                       `Complete list of valid values <https://github.com/Recidiviz/pulse-data/blob/main/recidiviz/common/constants/person.py#L65>`__
**ethnicity**          A person’s reported ethnicity.

                       Valid values:

                       -  Hispanic

                       -  Not Hispanic

                       `Complete list of valid values <https://github.com/Recidiviz/pulse-data/blob/main/recidiviz/common/constants/person.py#L78>`__
**place_of_residence** https://github.com/Recidiviz/pulse-data/issues/415
**bookings**           A list of Booking objects for this person.
====================== =============================================================================================================================================================================================================================

Booking
-------

The Booking object represents information about a single “stay” in jail, prison, or community supervision (e.g. probation or pretrial electronic monitoring). There is basic information about the booking (e.g., admission date and custody status), as well a list of charges that may be associated with the booking. In addition, information about the arrest that led to this booking is linked from here.

========================== ==================================================================================================================================================
**Field Name**             **Description**
========================== ==================================================================================================================================================
**booking_id**             Unique identifier for a booking. If not specified, one will be generated automatically.
**admission_date**         The date this person was booked into this facility.
**projected_release_date** The date this person is scheduled to be released on this booking.

                           This value is valid while someone is incarcerated, but release_date is filled in when someone is actually released. These values may be different.
**release_date**           The date this person was released on this booking.
**release_reason**         The reason the person was released.

                           Valid values:

                           -  Acquittal

                           -  Bond

                           -  Case Dismissed

                           -  Death

                           -  Escape

                           -  Expiration of Sentence

                           -  Own Recognizance

                           -  Parole

                           -  Probation

                           -  Transfer

                           `Complete list of valid values <https://github.com/Recidiviz/pulse-data/blob/main/recidiviz/common/constants/booking.py#L82>`__
**custody_status**         The custody status of the person with respect to this booking.

                           Valid values:

                           -  Escaped

                           -  Held Elsewhere

                           -  In Custody

                           -  Released

                           `Complete list of valid values <https://github.com/Recidiviz/pulse-data/blob/main/recidiviz/common/constants/booking.py#L73>`__
**facility**               The name of the facility the person is (or was) being held in on this booking.
**classification**         Security classification of the person with respect to this booking.

                           Valid values:

                           -  Maximum

                           -  High

                           -  Medium

                           -  Low

                           -  Minimum

                           -  Work Release

                           `Complete list of valid values <https://github.com/Recidiviz/pulse-data/blob/main/recidiviz/common/constants/booking.py#L63>`__
**total_bond_amount**      Sum of all bonds associated with this booking. This accommodates when only total bond is available, instead of bonds for individual charges.
**arrest**                 Link to the Arrest object, which contains information about the arrest that led to this booking.
**charges**                A list of Charge objects associated with this
                           booking.
**holds**                  A list of Hold objects associated with this booking.
========================== ==================================================================================================================================================

Arrest
------

The Arrest object represents information about the arrest that led to the associated booking.

================ =======================================================================================
**Field Name**   **Description**
================ =======================================================================================
**arrest_id**    Unique identifier for an arrest. If not specified, one will be generated automatically.
**arrest_date**  The date this person was arrested on this booking.
**location**     The location of arrest.
**officer_name** The name of the arresting officer.
**officer_id**   The ID of the arresting officer (e.g., badge number).
**agency**       The arresting agency (e.g., police department, Sheriff’s office).
================ =======================================================================================

Charge
------

The Charge object holds information on a single charge. Each booking may have several charges.

==================== ====================================================================================================================================
**Field Name**       **Description**
==================== ====================================================================================================================================
**charge_id**        Unique identifier for a charge. If not specified, one will be generated automatically.
**offense_date**     The date of the alleged offense that led to this charge.
**statute**          The identifier of the charge in the state or federal code.
**name**             Text description of the charge.
**attempted**        Whether this charge was an attempt or not (e.g., attempted murder).
**degree**           Charge degree.

                     Valid values:

                     -  First

                     -  Second

                     -  Third

                     `Complete list of valid values <https://github.com/Recidiviz/pulse-data/blob/main/recidiviz/common/constants/charge.py#L69>`__
**charge_class**     Charge class.

                     Valid values:

                     -  Civil

                     -  Felony

                     -  Misdemeanor

                     -  Parole violation

                     -  Probation violation

                     `Complete list of valid values <https://github.com/Recidiviz/pulse-data/blob/main/recidiviz/common/constants/charge.py#L79>`__
**level**            Charge level (e.g. 1, 2, A, B, etc.).
**fee_dollars**      Fee associated with this charge (e.g., booking fee, court fee).

                     Note, this is different than a fine, which is imposed as part of a sentence.
**charging_entity**  The entity that brought this charge (e.g., Boston Police Department, Southern District of New York).
**status**           Charge status.

                     Valid values:

                     -  Acquitted

                     -  Completed Sentence

                     -  Convicted

                     -  Dropped

                     -  Pending

                     -  Pretrial

                     -  Sentenced

                     `Complete list of valid values <https://github.com/Recidiviz/pulse-data/blob/main/recidiviz/common/constants/charge.py#L87>`__
**number_of_counts** The number of counts for this charge.
**court_type**       The type of court this charge will be / was heard in.
**case_number**      Court case number for this charge.
**next_court_date**  Date of the next scheduled court appearance on this charge.
**judge_name**       Name of the judge who will hear this case.
**charge_notes**     Free text containing other information about a charge.
**bond**             A link to the Bond object associated with this charge.
**sentence**         A link to the Sentence object associated with this charge.
==================== ====================================================================================================================================

Hold
----

A Hold object holds information on a hold. This usually means someone has
charges in another jurisdiction (like a state or county), so that
jurisdiction "has a hold on" the individual.

===================== ==========================================================
**Field Name**        **Description**
===================== ==========================================================
**hold_id**           Unique identifier for a hold.
**jurisdiction_name** The name of the jurisdiction that the hold originates
                      from.
**status**            Status of the hold.

                      Valid values:

                      -  Active

                      -  Inactive

                      `Complete list of valid values <https://github.com/Recidiviz/pulse-data/blob/main/recidiviz/common/constants/hold.py>`__
===================== ==========================================================

Bond
----

A Bond object holds information on a bond. A bond can be per charge (each charge will have one bond object associated with it), apply to multiple charges (multiple charges point to the same bond), or be a total bond across the whole booking (just means all the charges for the booking are associated with the same bond).

============== ==================================================================================================================================
**Field Name** **Description**
============== ==================================================================================================================================
**bond_id**    Unique identifier for a bond.
**amount**     Dollar amount of this bond.
**bond_type**  Type of bond.

               Valid values:

               -  Bond Denied

               -  Cash

               -  No Bond

               -  Secured

               -  Unsecured

               `Complete list of valid values <https://github.com/Recidiviz/pulse-data/blob/main/recidiviz/common/constants/bond.py#L44>`__
**status**     The status of this bond.

               Valid values:

               -  Active

               -  Posted

               `Complete list of valid values <https://github.com/Recidiviz/pulse-data/blob/main/recidiviz/common/constants/bond.py#L52>`__
============== ==================================================================================================================================

Sentence
--------

A Sentence object holds information about a sentence imposed for one or more charges.

=================================== ========================================================================================
**Field Name**                      **Description**
=================================== ========================================================================================
**sentence_id**                     Unique identifier for a sentence. If not specified, one will be generated automatically.
**date_imposed**                    Sentencing date.
**status**                          Sentencing status.
**sentencing_region**               https://github.com/Recidiviz/pulse-data/issues/419

                                    The place that imposed the sentence.
**min_length**                      Minimum duration of the sentence.
**max_length**                      Maximum duration of the sentence.
**is_life**                         Flag indicating that the sentence is a life sentence.
**is_probation**                    Flag indicating that the sentence is just a probation sentence.
**is_suspended**                    Flag indicating that the sentence is suspended.
**fine_dollars**                    Fine amount imposed as part of this sentence.
**parole_possible**                 Flag indicating whether parole is a possibility
**post_release_supervision_length** Duration of community supervision to be served after release from incarceration.
**projected_completion_date**       The date the sentence is expected to have been completed.
                                    This value is valid while someone is incarcerated, but completion_date is filled in when someone is actually released. These values may be different.
**completion_date**                 The date this sentence was completed. This should only be filled in if it is in the past.
=================================== ========================================================================================

SentenceRelationship
--------------------

A SentenceRelationship object holds information about the relationship between two sentences.

============================ =====================================================================================================
**Field Name**               **Description**
============================ =====================================================================================================
**sentence_relationship_id** Unique identifier for a sentence relationship. If not specified, one will be generated automatically.
**sentence_a**               A link to one of the two sentences in the relationship (order does not matter).
**sentence_b**               A link to the other of the two sentences in the relationship (order does not matter).
**relationship_type**        The type of relationship between the two sentences.

                             Valid values:

                             - Concurrent

                             - Consecutive
============================ =====================================================================================================


