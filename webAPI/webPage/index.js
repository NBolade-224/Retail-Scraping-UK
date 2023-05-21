// Resize all elements Function
function resize() {
    var newWidth = window.screen.width;
    var newHeight = window.innerHeight;
    var newWidth = innerWidth;
    var newHeight = innerHeight;
    console.log(`width = ${newWidth}`)
    console.log(`height = ${newHeight}`)
    var AllContainers = document.getElementsByClassName("page1");
    if (newWidth > 600) {
        for (let i = 0; i < AllContainers.length; i++) {
        AllContainers[i].style.height = `${newHeight}px`;
        }
    };
};

// Sumbit Button Function
function sumbitButton() {
  var searchtype = document.getElementById("searchTable").value;
  var input = document.getElementById("SearchBar").value;
  fetchMovies(searchtype,input);
};

// Async Fetch (awaits before continuing)
async function fetchMovies(searchtable,input) {
  const response = await fetch(`https://frostedcornflakes.com/apiEndpoint?searchtable=${searchtable}&input=${input}`);
  const data = await response.json();
  var yValues = data[0]
  var xValues = data[1]
  var newTitle = data[2]
  console.log(data[0]);
  console.log(data[1]);

  // Change Product Title
  var spanTitle = document.getElementById("Title");
  spanTitle.textContent = newTitle;

  // Generate Graph
  new Chart("graph", {
      type: "line",
      data: {
        labels: xValues,
        datasets: [{
          fill: false,
          lineTension: 0,
          backgroundColor: "black",
          borderColor: "red",
          data: yValues
        }]
      },
      options: {
        legend: {display: false},
      }
    });
  Chart.defaults.global.defaultFontColor = "white";
}

// Listener to Resize all elements
window.addEventListener("DOMContentLoaded", 
    resize
);

// log to confirm JavaScript has fully loaded
console.log(`Java Script Load Complete`);

// // Type into Search Bar Listener
// var searchBarInputID = document.getElementById("SearchBar")
// searchBarInputID.addEventListener("input", function(e) {
//     console.log(`Typed into search bar`);
// });

// Click on screen listener
// document.addEventListener("click", function (e) {
//     console.log(`Clicked on screen`);
// });