import java.sql.*;
import java.util.Date;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;

public class Assignment2 {

	// A connection to the database
	Connection connection;

	// Can use if you wish: seat letters
	List<String> seatLetters = Arrays.asList("A", "B", "C", "D", "E", "F");

	Assignment2() throws SQLException {
		try {
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Connects and sets the search path.
	 *
	 * Establishes a connection to be used for this session, assigning it to
	 * the instance variable 'connection'.  In addition, sets the search
	 * path to 'air_travel, public'.
	 *
	 * @param  url       the url for the database
	 * @param  username  the username to connect to the database
	 * @param  password  the password to connect to the database
	 * @return           true if connecting is successful, false otherwise
	 */
	public boolean connectDB(String URL, String username, String password) {
		try {
			connection = DriverManager.getConnection(URL, username, password);
			PreparedStatement ps = connection.prepareStatement(
				"set search_path to air_travel, public"
			);
			ps.executeUpdate();
			ps.close();
			return true;
		} catch (SQLException se) {
			se.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return false;
	}

	/**
	 * Closes the database connection.
	 *
	 * @return true if the closing was successful, false otherwise
	 */
	public boolean disconnectDB() {
		try {
			connection.close();
			return true;
		} catch (SQLException se) {
			se.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return false;
	}

	/* ======================= Airline-related methods ======================= */

	/**
	* Attempts to book a flight for a passenger in a particular seat class. 
	* Does so by inserting a row into the Booking table.
	*
	* Read handout for information on how seats are booked.
	* Returns false if seat can't be booked, or if passenger or flight cannot be found.
	* 
	* @param  passID     id of the passenger
	* @param  flightID   id of the flight
	* @param  seatClass  the class of the seat (economy, business, or first) 
	* @return            true if the booking was successful, false otherwise. 
	*/
	public boolean bookSeat(int passID, int flightID, String seatClass) {
		try {
			// check if passenger passID exists. if not, return false
			PreparedStatement ps = connection.prepareStatement(
				"select id from Passenger where id = ?"
			);
			ps.setInt(1, passID);
			ResultSet rs = ps.executeQuery();
			if (rs.isBeforeFirst() == false) { // passenger does not exist (there are no rows in rs)
				System.out.println("this passenger does not exist. cannot be booked");
				return false;
			}
			ps.close();

			// check if flight flightID exists. if not, return false
			ps = connection.prepareStatement(
				"select id from Flight where id = ?"
			);
			ps.setInt(1, flightID);
			rs = ps.executeQuery();
			if (rs.isBeforeFirst() == false) { // flight does not exist (there are no rows in rs)
				System.out.println("this flight does not exist. cannot be booked");
				return false;
			}
			ps.close();

			// find the capacity of each seat class on the flightID plane
			int capacity_first = 0;
			int capacity_business = 0;
			int capacity_economy = 0;
			ps = connection.prepareStatement(
				"select Plane.capacity_first as capacity_first, " +
					"Plane.capacity_business as capacity_business, " +
					"Plane.capacity_economy as capacity_economy " +
				"from Flight " +
					"join Plane on Flight.plane = Plane.tail_number " +
				"where Flight.id = ?"
			);
			ps.setInt(1, flightID);
			rs = ps.executeQuery();
			while (rs.next()) { // row by row
				capacity_first = rs.getInt("capacity_first");
				capacity_business = rs.getInt("capacity_business");
				capacity_economy = rs.getInt("capacity_economy");
			}
			ps.close();

			// find the number of currently booked seats of each seat class on the flightID flight

			int num_curr_booked_seats_first = 0;
			ps = connection.prepareStatement(
				"select count(seat_class) as num_curr_booked_seats_first " +
					"from Booking " +
				"where seat_class = 'first' " +
					"and flight_id = ?"
			);
			ps.setInt(1, flightID);
			rs = ps.executeQuery();
			while (rs.next()) { // row by row
				num_curr_booked_seats_first = rs.getInt("num_curr_booked_seats_first");
			}
			ps.close();

			int num_curr_booked_seats_business = 0;
			ps = connection.prepareStatement(
				"select count(seat_class) as num_curr_booked_seats_business " +
					"from Booking " +
				"where seat_class = 'business' " +
					"and flight_id = ?"
			);
			ps.setInt(1, flightID);
			rs = ps.executeQuery();
			while (rs.next()) { // row by row
				num_curr_booked_seats_business = rs.getInt("num_curr_booked_seats_business");
			}
			ps.close();

			int num_curr_booked_seats_economy = 0;
			ps = connection.prepareStatement(
				"select count(seat_class) as num_curr_booked_seats_economy " +
					"from Booking " +
				"where seat_class = 'economy' " +
					"and row is not NULL " +
					"and letter is not NULL " +
					"and flight_id = ?"
			);
			ps.setInt(1, flightID);
			rs = ps.executeQuery();
			while (rs.next()) { // row by row
				num_curr_booked_seats_economy = rs.getInt("num_curr_booked_seats_economy");
			}
			ps.close();

			// find the number of free seats in each seat class of flight flightID
			int num_free_seats_first = capacity_first - num_curr_booked_seats_first;
			int num_free_seats_business = capacity_business - num_curr_booked_seats_business;
			int num_free_seats_economy = capacity_economy - num_curr_booked_seats_economy;

			// find the highest currently booked row of each seat class on the flightID flight

			int highest_curr_booked_row_first = 0;
			ps = connection.prepareStatement(
				"select max(row) as highest_curr_booked_row_first " +
				"from Booking " +
				"where flight_id = ? " +
					"and seat_class = 'first'"
			);
			ps.setInt(1, flightID);
			rs = ps.executeQuery();
			while (rs.next()) { // row by row
				highest_curr_booked_row_first = rs.getInt("highest_curr_booked_row_first");
			}
			ps.close();

			int highest_curr_booked_row_business = 0;
			ps = connection.prepareStatement(
				"select max(row) as highest_curr_booked_row_business " +
				"from Booking " +
				"where flight_id = ? " +
					"and seat_class = 'business'"
			);
			ps.setInt(1, flightID);
			rs = ps.executeQuery();
			while (rs.next()) { // row by row
				highest_curr_booked_row_business = rs.getInt("highest_curr_booked_row_business");
			}
			ps.close();

			int highest_curr_booked_row_economy = 0;
			ps = connection.prepareStatement(
				"select max(row) as highest_curr_booked_row_economy " +
				"from Booking " +
				"where flight_id = ? " +
					"and seat_class = 'economy'"
			);
			ps.setInt(1, flightID);
			rs = ps.executeQuery();
			while (rs.next()) { // row by row
				highest_curr_booked_row_economy = rs.getInt("highest_curr_booked_row_economy");
			}
			ps.close();

			// find the highest currently booked letter (relative to the highest currently booked row) of each seat class on the flightID flight

			char highest_curr_booked_letter_first = '\0';
			ps = connection.prepareStatement(
				"select max(letter) as highest_curr_booked_letter_first " +
				"from Booking " +
				"where flight_id = ? " +
					"and row = ? " +
					"and seat_class = 'first'"
			);
			ps.setInt(1, flightID);
			ps.setInt(2, highest_curr_booked_row_first);
			rs = ps.executeQuery();
			while (rs.next()) { // row by row
				highest_curr_booked_letter_first = rs.getString("highest_curr_booked_letter_first").charAt(0);
			}
			ps.close();

			char highest_curr_booked_letter_business = '\0';
			ps = connection.prepareStatement(
				"select max(letter) as highest_curr_booked_letter_business " +
				"from Booking " +
				"where flight_id = ? " +
					"and row = ? " +
					"and seat_class = 'business'"
			);
			ps.setInt(1, flightID);
			ps.setInt(2, highest_curr_booked_row_business);
			rs = ps.executeQuery();
			while (rs.next()) { // row by row
				highest_curr_booked_letter_business = rs.getString("highest_curr_booked_letter_business").charAt(0);
			}
			ps.close();

			char highest_curr_booked_letter_economy = '\0';
			ps = connection.prepareStatement(
				"select max(letter) as highest_curr_booked_letter_economy " +
				"from Booking " +
				"where flight_id = ? " +
					"and row = ? " +
					"and seat_class = 'economy'"
			);
			ps.setInt(1, flightID);
			ps.setInt(2, highest_curr_booked_row_economy);
			rs = ps.executeQuery();
			while (rs.next()) { // row by row
				highest_curr_booked_letter_economy = rs.getString("highest_curr_booked_letter_economy").charAt(0);
			}
			ps.close();

			// find out whether economy class is fully overbooked or not on the flightID flight

			int num_of_overbooked = 0;
			ps = connection.prepareStatement(
				"select count(id) as num_of_overbooked " +
				"from Booking " +
				"where flight_id = ? " +
					"and row is NULL " +
					"and letter is NULL"
			);
			ps.setInt(1, flightID);
			rs = ps.executeQuery();
			while (rs.next()) { // row by row
				num_of_overbooked = rs.getInt("num_of_overbooked");
			}
			ps.close();

			boolean fully_overbooked = false;
			if (num_of_overbooked == 10) {
				fully_overbooked = true;
			} else {
				fully_overbooked = false;
			}

			// book a seatClass seat on flight flightID, if it can be booked
			if (seatClass.equals("first")) {
				if (num_free_seats_first == 0) {
					System.out.println("cannot be booked");
					return false;
				} else {
					int row_to_insert = 0;
					if (highest_curr_booked_letter_first != 'F') {
						row_to_insert = highest_curr_booked_row_first;
					} else {
						row_to_insert = highest_curr_booked_row_first + 1;
					}
					
					char letter_to_insert = '\0';
					if (highest_curr_booked_letter_first == 'A') {
						letter_to_insert = 'B';
					} else if (highest_curr_booked_letter_first == 'B') {
						letter_to_insert = 'C';
					} else if (highest_curr_booked_letter_first == 'C') {
						letter_to_insert = 'D';
					} else if (highest_curr_booked_letter_first == 'D') {
						letter_to_insert = 'E';
					} else if (highest_curr_booked_letter_first == 'E') {
						letter_to_insert = 'F';
					} else if (highest_curr_booked_letter_first == 'F') {
						letter_to_insert = 'A';
					}

					ps.close();
					ps = connection.prepareStatement(
						"insert into Booking " +
						"values ((select (max(id) + 1) from Booking), " +
							"?, " +
							"?, " +
							"?, " +
							"(select first from Price where flight_id = ?), " +
							"'first', " +
							"?, " +
							"?)"
					);
					ps.setInt(1, passID);
					ps.setInt(2, flightID);
					ps.setTimestamp(3, getCurrentTimeStamp());
					ps.setInt(4, flightID);
					ps.setInt(5, row_to_insert);
					ps.setString(6, String.valueOf(letter_to_insert));
					ps.executeUpdate();
					ps.close();

					System.out.println("booking successful");
					return true;
				}
			} else if (seatClass.equals("business")) {
				if (num_free_seats_business == 0) {
					System.out.println("cannot be booked");
					return false;
				} else {
					int row_to_insert = 0;
					if (highest_curr_booked_letter_business != 'F') {
						row_to_insert = highest_curr_booked_row_business;
					} else {
						row_to_insert = highest_curr_booked_row_business + 1;
					}
					
					char letter_to_insert = '\0';
					if (highest_curr_booked_letter_business == 'A') {
						letter_to_insert = 'B';
					} else if (highest_curr_booked_letter_business == 'B') {
						letter_to_insert = 'C';
					} else if (highest_curr_booked_letter_business == 'C') {
						letter_to_insert = 'D';
					} else if (highest_curr_booked_letter_business == 'D') {
						letter_to_insert = 'E';
					} else if (highest_curr_booked_letter_business == 'E') {
						letter_to_insert = 'F';
					} else if (highest_curr_booked_letter_business == 'F') {
						letter_to_insert = 'A';
					}

					ps.close();
					ps = connection.prepareStatement(
						"insert into Booking " +
						"values ((select (max(id) + 1) from Booking), " +
							"?, " +
							"?, " +
							"?, " +
							"(select business from Price where flight_id = ?), " +
							"'business', " +
							"?, " +
							"?)"
					);
					ps.setInt(1, passID);
					ps.setInt(2, flightID);
					ps.setTimestamp(3, getCurrentTimeStamp());
					ps.setInt(4, flightID);
					ps.setInt(5, row_to_insert);
					ps.setString(6, String.valueOf(letter_to_insert));
					ps.executeUpdate();
					ps.close();

					System.out.println("booking successful");
					return true;
				}
			} else if (seatClass.equals("economy")) {
				if ((num_free_seats_economy == 0) && (fully_overbooked == true)) {
					System.out.println("cannot be booked");
					return false;
				} else if ((num_free_seats_economy == 0) && (fully_overbooked == false)) {
					ps.close();
					ps = connection.prepareStatement(
						"insert into Booking " +
						"values ((select (max(id) + 1) from Booking), " +
							"?, " +
							"?, " +
							"?, " +
							"(select economy from Price where flight_id = ?), " +
							"'economy', " +
							"NULL, " +
							"NULL)"
					);
					ps.setInt(1, passID);
					ps.setInt(2, flightID);
					ps.setTimestamp(3, getCurrentTimeStamp());
					ps.setInt(4, flightID);
					ps.executeUpdate();
					ps.close();

					System.out.println("booking successful");
					return true;
				} else {
					int row_to_insert = 0;
					if (highest_curr_booked_letter_economy != 'F') {
						row_to_insert = highest_curr_booked_row_economy;
					} else {
						row_to_insert = highest_curr_booked_row_economy + 1;
					}
					
					char letter_to_insert = '\0';
					if (highest_curr_booked_letter_economy == 'A') {
						letter_to_insert = 'B';
					} else if (highest_curr_booked_letter_economy == 'B') {
						letter_to_insert = 'C';
					} else if (highest_curr_booked_letter_economy == 'C') {
						letter_to_insert = 'D';
					} else if (highest_curr_booked_letter_economy == 'D') {
						letter_to_insert = 'E';
					} else if (highest_curr_booked_letter_economy == 'E') {
						letter_to_insert = 'F';
					} else if (highest_curr_booked_letter_economy == 'F') {
						letter_to_insert = 'A';
					}

					ps.close();
					ps = connection.prepareStatement(
						"insert into Booking " +
						"values ((select (max(id) + 1) from Booking), " +
							"?, " +
							"?, " +
							"?, " +
							"(select economy from Price where flight_id = ?), " +
							"'economy', " +
							"?, " +
							"?)"
					);
					ps.setInt(1, passID);
					ps.setInt(2, flightID);
					ps.setTimestamp(3, getCurrentTimeStamp());
					ps.setInt(4, flightID);
					ps.setInt(5, row_to_insert);
					ps.setString(6, String.valueOf(letter_to_insert));
					ps.executeUpdate();
					ps.close();

					System.out.println("booking successful");
					return true;
				}
			}
		} catch (SQLException se) {
			se.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return false;
	}

	/**
	* Attempts to upgrade overbooked economy passengers to business class
	* or first class (in that order until each seat class is filled).
	* Does so by altering the database records for the bookings such that the
	* seat and seat_class are updated if an upgrade can be processed.
	*
	* Upgrades should happen in order of earliest booking timestamp first.
	*
	* If economy passengers left over without a seat (i.e. more than 10 overbooked
	* passengers or not enough higher class seats), remove their bookings from the
	* database.
	* 
	* @param  flightID  The flight to upgrade passengers in.
	* @return           the number of passengers upgraded, or -1 if an error occured.
	*/
	public int upgrade(int flightID) {
		try {
			// check if flight flightID exists. if not, return -1
			PreparedStatement ps = connection.prepareStatement(
				"select id from Flight where id = ?"
			);
			ps.setInt(1, flightID);
			ResultSet rs = ps.executeQuery();
			if (rs.isBeforeFirst() == false) { // flight does not exist (there are no rows in rs)
				System.out.println("this flight does not exist. cannot upgrade");
				return -1;
			}
			ps.close();

			// check if there are any overbooked passengers on flight flightID
			int num_overbooked = 0;
			ps = connection.prepareStatement(
				"select count(*) as num_overbooked " +
				"from Booking " +
				"where flight_id = ? " +
					"and row is NULL " +
					"and letter is NULL"
			);
			ps.setInt(1, flightID);
			rs = ps.executeQuery();
			while (rs.next()) { // row by row
				num_overbooked = rs.getInt("num_overbooked");
			}
			ps.close();
			if (num_overbooked == 0) { // there are no overbooked passengers on flight flightID
				System.out.println("there are no overbooked passengers on this flight");
				return 0;
			}

			// find the capacity of business class and first class on the flightID plane
			int capacity_business = 0;
			int capacity_first = 0;
			ps = connection.prepareStatement(
				"select Plane.capacity_business as capacity_business, " +
					"Plane.capacity_first as capacity_first " +
				"from Flight " +
					"join Plane on Flight.plane = Plane.tail_number " +
				"where Flight.id = ?"
			);
			ps.setInt(1, flightID);
			rs = ps.executeQuery();
			while (rs.next()) { // row by row
				capacity_business = rs.getInt("capacity_business");
				capacity_first = rs.getInt("capacity_first");
			}
			ps.close();

			// find the number of currently booked seats of business class and first class on the flightID flight

			int num_curr_booked_seats_business = 0;
			ps = connection.prepareStatement(
				"select count(seat_class) as num_curr_booked_seats_business " +
					"from Booking " +
				"where seat_class = 'business' " +
					"and flight_id = ?"
			);
			ps.setInt(1, flightID);
			rs = ps.executeQuery();
			while (rs.next()) { // row by row
				num_curr_booked_seats_business = rs.getInt("num_curr_booked_seats_business");
			}
			ps.close();

			int num_curr_booked_seats_first = 0;
			ps = connection.prepareStatement(
				"select count(seat_class) as num_curr_booked_seats_first " +
					"from Booking " +
				"where seat_class = 'first' " +
					"and flight_id = ?"
			);
			ps.setInt(1, flightID);
			rs = ps.executeQuery();
			while (rs.next()) { // row by row
				num_curr_booked_seats_first = rs.getInt("num_curr_booked_seats_first");
			}
			ps.close();

			// check if there are free seats in business class and in first class of flight flightID

			int num_free_seats_business = capacity_business - num_curr_booked_seats_business;
			int num_free_seats_first = capacity_first - num_curr_booked_seats_first;
			int max_possible_upgrades = num_free_seats_business + num_free_seats_first;

			if (max_possible_upgrades == 0) { // there are no free seats on flight flightID
				ps = connection.prepareStatement(
					"delete from Booking " +
					"where flight_id = ? " +
						"and row is NULL " +
						"and letter is NULL"
				);
				ps.setInt(1, flightID);
				ps.executeUpdate();
				ps.close();

				System.out.println("there are no free seats on this flight");
				return 0;
			}

			// get a list of all the overbooked passengers by their booking id's on flight flightID and order them ascending by earliest booking datetime
			List<Integer> overbooked_passengers = new ArrayList<>();
			ps.close();
			ps = connection.prepareStatement(
				"select id as booking_id " +
				"from Booking " +
				"where row is NULL " +
					"and letter is NULL " +
					"and flight_id = ? " +
				"order by datetime asc"
			);
			ps.setInt(1, flightID);
			rs = ps.executeQuery();
			while (rs.next()) { // row by row
				overbooked_passengers.add(rs.getInt("booking_id"));
			}
			ps.close();

			// find the highest currently booked row of business class and first class on the flightID flight

			int highest_curr_booked_row_business = 0;
			ps = connection.prepareStatement(
				"select max(row) as highest_curr_booked_row_business " +
				"from Booking " +
				"where flight_id = ? " +
					"and seat_class = 'business'"
			);
			ps.setInt(1, flightID);
			rs = ps.executeQuery();
			while (rs.next()) { // row by row
				highest_curr_booked_row_business = rs.getInt("highest_curr_booked_row_business");
			}
			ps.close();

			int highest_curr_booked_row_first = 0;
			ps = connection.prepareStatement(
				"select max(row) as highest_curr_booked_row_first " +
				"from Booking " +
				"where flight_id = ? " +
					"and seat_class = 'first'"
			);
			ps.setInt(1, flightID);
			rs = ps.executeQuery();
			while (rs.next()) { // row by row
				highest_curr_booked_row_first = rs.getInt("highest_curr_booked_row_first");
			}
			ps.close();

			// find the highest currently booked letter (relative to the highest currently booked row) of business class and first class on the flightID flight

			char highest_curr_booked_letter_business = '\0';
			ps = connection.prepareStatement(
				"select max(letter) as highest_curr_booked_letter_business " +
				"from Booking " +
				"where flight_id = ? " +
					"and row = ? " +
					"and seat_class = 'business'"
			);
			ps.setInt(1, flightID);
			ps.setInt(2, highest_curr_booked_row_business);
			rs = ps.executeQuery();
			while (rs.next()) { // row by row
				highest_curr_booked_letter_business = rs.getString("highest_curr_booked_letter_business").charAt(0);
			}
			ps.close();

			char highest_curr_booked_letter_first = '\0';
			ps = connection.prepareStatement(
				"select max(letter) as highest_curr_booked_letter_first " +
				"from Booking " +
				"where flight_id = ? " +
					"and row = ? " +
					"and seat_class = 'first'"
			);
			ps.setInt(1, flightID);
			ps.setInt(2, highest_curr_booked_row_first);
			rs = ps.executeQuery();
			while (rs.next()) { // row by row
				highest_curr_booked_letter_first = rs.getString("highest_curr_booked_letter_first").charAt(0);
			}
			ps.close();

			// upgrade overbooked passenger(s) on flight flightID

			int upgrade_count = 0;

			int i = 0;
			while ((i < num_free_seats_business)
				&& (upgrade_count < num_overbooked)
				&& (upgrade_count < max_possible_upgrades)) { // upgrade to business class
				int modify_row_to = 0;
				if (highest_curr_booked_letter_business != 'F') {
					modify_row_to = highest_curr_booked_row_business;
				} else {
					modify_row_to = highest_curr_booked_row_business + 1;
					highest_curr_booked_row_business += 1;
				}

				char modify_letter_to = '\0';
				if (highest_curr_booked_letter_business == 'A') {
					modify_letter_to = 'B';
					highest_curr_booked_letter_business = 'B';
				} else if (highest_curr_booked_letter_business == 'B') {
					modify_letter_to = 'C';
					highest_curr_booked_letter_business = 'C';
				} else if (highest_curr_booked_letter_business == 'C') {
					modify_letter_to = 'D';
					highest_curr_booked_letter_business = 'D';
				} else if (highest_curr_booked_letter_business == 'D') {
					modify_letter_to = 'E';
					highest_curr_booked_letter_business = 'E';
				} else if (highest_curr_booked_letter_business == 'E') {
					modify_letter_to = 'F';
					highest_curr_booked_letter_business = 'F';
				} else if (highest_curr_booked_letter_business == 'F') {
					modify_letter_to = 'A';
					highest_curr_booked_letter_business = 'A';
				}

				ps.close();
				ps = connection.prepareStatement(
					"update Booking " +
					"set seat_class = 'business', " +
						"row = ?, " +
						"letter = ? " +
					"where id = ?"
				);
				ps.setInt(1, modify_row_to);
				ps.setString(2, String.valueOf(modify_letter_to));
				ps.setInt(3, overbooked_passengers.get(upgrade_count));
				ps.executeUpdate();
				ps.close();
				upgrade_count += 1;
				
				i += 1;
			}

			i = 0;
			while ((i < num_free_seats_first)
				&& (upgrade_count < num_overbooked)
				&& (upgrade_count < max_possible_upgrades)) { // upgrade to first class
				int modify_row_to = 0;
				if (highest_curr_booked_letter_first != 'F') {
					modify_row_to = highest_curr_booked_row_first;
				} else {
					modify_row_to = highest_curr_booked_row_first + 1;
					highest_curr_booked_row_first += 1;
				}

				char modify_letter_to = '\0';
				if (highest_curr_booked_letter_first == 'A') {
					modify_letter_to = 'B';
					highest_curr_booked_letter_first = 'B';
				} else if (highest_curr_booked_letter_first == 'B') {
					modify_letter_to = 'C';
					highest_curr_booked_letter_first = 'C';
				} else if (highest_curr_booked_letter_first == 'C') {
					modify_letter_to = 'D';
					highest_curr_booked_letter_first = 'D';
				} else if (highest_curr_booked_letter_first == 'D') {
					modify_letter_to = 'E';
					highest_curr_booked_letter_first = 'E';
				} else if (highest_curr_booked_letter_first == 'E') {
					modify_letter_to = 'F';
					highest_curr_booked_letter_first = 'F';
				} else if (highest_curr_booked_letter_first == 'F') {
					modify_letter_to = 'A';
					highest_curr_booked_letter_first = 'A';
				}

				ps.close();
				ps = connection.prepareStatement(
					"update Booking " +
					"set seat_class = 'first', " +
						"row = ?, " +
						"letter = ? " +
					"where id = ?"
				);
				ps.setInt(1, modify_row_to);
				ps.setString(2, String.valueOf(modify_letter_to));
				ps.setInt(3, overbooked_passengers.get(upgrade_count));
				ps.executeUpdate();
				ps.close();
				upgrade_count += 1;
				
				i += 1;
			}

			// remove all remaining overbooked passengers on flight flightID from Booking
			ps = connection.prepareStatement(
				"delete from Booking " +
				"where flight_id = ? " +
					"and row is NULL " +
					"and letter is NULL"
			);
			ps.setInt(1, flightID);
			ps.executeUpdate();
			ps.close();
			
			// done
			if (upgrade_count == 1) {
				System.out.println("upgrade complete with [" + upgrade_count + "] passenger upgraded");
			} else {
				System.out.println("upgrade complete with [" + upgrade_count + "] passengers upgraded");
			}
			return upgrade_count;
		} catch (SQLException se) {
			se.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return -1;
	}

	/* ----------------------- Helper functions below  ------------------------- */

	// A helpful function for adding a timestamp to new bookings.
	// Example of setting a timestamp in a PreparedStatement:
	// ps.setTimestamp(1, getCurrentTimeStamp());

	/**
	* Returns a SQL Timestamp object of the current time.
	* 
	* @return           Timestamp of current time.
	*/
	private java.sql.Timestamp getCurrentTimeStamp() {
		java.util.Date now = new java.util.Date();
		return new java.sql.Timestamp(now.getTime());
	}

	// Add more helper functions below if desired.

  
	/* ----------------------- Main method below  ------------------------- */

	public static void main(String[] args) {
		System.out.println("Running the code!");
		// You can put testing code in here. It will not affect our autotester.
		try {
			Assignment2 a2 = new Assignment2();
			a2.connectDB("jdbc:postgresql://localhost:5432/csc343h-chanja54", "chanja54", "");

			a2.bookSeat(1, 10, "economy");
			// a2.upgrade(10);

			a2.disconnectDB();
		} catch (SQLException se) {
			se.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
